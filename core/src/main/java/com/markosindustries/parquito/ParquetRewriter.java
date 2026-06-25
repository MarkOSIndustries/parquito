package com.markosindustries.parquito;

import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.predicates.ParquetPredicate;
import com.markosindustries.parquito.rows.NoOpFieldVisitor;
import com.markosindustries.parquito.rows.OptionalBranchIterator;
import com.markosindustries.parquito.rows.PushdownPredicates;
import java.io.IOException;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.parquet.format.RowGroup;

public class ParquetRewriter {
  private final Function<RowGroupReader, ParquetPredicate> keepRowsPredicateProvider;

  public ParquetRewriter(
      final Function<RowGroupReader, ParquetPredicate> keepRowsPredicateProvider) {
    this.keepRowsPredicateProvider = keepRowsPredicateProvider;
  }

  private sealed interface RowGroupAction
      permits RowGroupAction.Copy, RowGroupAction.Rewrite, RowGroupAction.Drop {
    record Copy(RowGroup rowGroup) implements RowGroupAction {}

    record Drop() implements RowGroupAction {}

    record Rewrite(RowGroup rowGroup, BitSet keepRowsBitset, long countOfRowsToKeep)
        implements RowGroupAction {}
  }

  /**
   * Rewrites a parquet file by only touching row groups that need modification according to the
   * keepRowsPredicate. Rows that match are copied across, while rows that don't are dropped. There
   * are three cases we handle:
   *
   * <ul>
   *   <li>Row groups where all rows match will be copied at the byte level without any semantic
   *       understanding of the contents.
   *   <li>Row groups where no rows match are dropped entirely
   *   <li>Row groups where some rows match are rewritten according to the provided {@link
   *       WriteSpec}
   * </ul>
   *
   * @param sourceRangeReader A byte range reader to read from the existing parquet file
   * @param targetOutputStream An output stream to write the filtered file to
   * @param writeSpecProvider Given a schema, provide a {@link WriteSpec}. This only applies to row
   *     groups that need to be modified.
   * @return A future which will complete once the file has been fully rewritten to the output
   *     stream
   */
  public CompletableFuture<Void> rewrite(
      final ByteRangeReader sourceRangeReader,
      final OutputStream targetOutputStream,
      final Function<ParquetSchemaNode.Root, WriteSpec> writeSpecProvider) {
    return ParquetFooter.read(sourceRangeReader)
        .thenAcceptAsync(
            footer -> {
              final var schema = ParquetSchemaNode.from(footer.schema);
              final var writeSpec = writeSpecProvider.apply(schema);
              try (final var writer =
                  new RowGroupWriter<>(
                      targetOutputStream, writeSpec, new NoOpWriter(footer.schema, schema))) {
                // Keeping this here to avoid re-allocating for each row group
                final var reusableBitSet = new BitSet();
                for (final var rowGroup : footer.row_groups) {
                  reusableBitSet.clear();
                  final var rowGroupAction =
                      determineRowGroupAction(rowGroup, schema, sourceRangeReader, reusableBitSet);

                  switch (rowGroupAction) {
                    case RowGroupAction.Copy copy ->
                        copyRowGroup(copy.rowGroup, sourceRangeReader, writer);
                    case RowGroupAction.Drop drop -> {}
                    case RowGroupAction.Rewrite rewrite ->
                        rewriteRowGroup(
                            schema,
                            sourceRangeReader,
                            new RowGroupReader(rewrite.rowGroup(), schema),
                            rewrite.keepRowsBitset(),
                            rewrite.countOfRowsToKeep(),
                            writer);
                  }
                }

                if (footer.isSetKey_value_metadata()) {
                  for (final var keyValueMetadatum : footer.key_value_metadata) {
                    writer.putMetaData(keyValueMetadatum.key, keyValueMetadatum.value);
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            Concurrency.DEFAULT_EXECUTOR);
  }

  private RowGroupAction determineRowGroupAction(
      final RowGroup rowGroup,
      final ParquetSchemaNode.Root schema,
      final ByteRangeReader sourceRangeReader,
      final BitSet keepRowsBitset) {
    final var rowGroupReader = new RowGroupReader(rowGroup, schema);
    final var keepRowsPredicate = keepRowsPredicateProvider.apply(rowGroupReader);

    boolean anyKeep = false, anyDiscard = false;

    final var pushdownPredicates = new PushdownPredicates(keepRowsPredicate);
    final var predicateIterator =
        new OptionalBranchIterator(
            rowGroupReader.makeFieldIterators(
                keepRowsPredicate.asSchemaTraversalSpec(),
                pushdownPredicates,
                schema,
                sourceRangeReader),
            schema);

    var rowIndex = 0;
    var countOfRowsToKeep = 0L;
    while (predicateIterator.hasNext()) {
      final var isMatch = pushdownPredicates.matchesNextRow();
      anyKeep = anyKeep || isMatch;
      anyDiscard = anyDiscard || !isMatch;
      keepRowsBitset.set(rowIndex++, isMatch);
      if (isMatch) {
        countOfRowsToKeep++;
      }
      predicateIterator.visitNext(NoOpFieldVisitor.INSTANCE);
    }

    if (anyKeep) {
      if (anyDiscard) {
        return new RowGroupAction.Rewrite(rowGroup, keepRowsBitset, countOfRowsToKeep);
      } else {
        return new RowGroupAction.Copy(rowGroup);
      }
    } else {
      return new RowGroupAction.Drop();
    }
  }

  private static void rewriteRowGroup(
      final ParquetSchemaNode.Root schemaRoot,
      final ByteRangeReader byteRangeReader,
      final RowGroupReader rowGroupReader,
      final BitSet rowsToKeep,
      final long countOfRowsToKeep,
      final RowGroupWriter<Object> writer)
      throws IOException {
    final var columnSchemaNodes = schemaRoot.findLeafNodes();
    final var rowGroupRangeReader = rowGroupReader.preloadRowGroup(byteRangeReader).join();

    final var valuePumps = new ValuePump[columnSchemaNodes.size()];
    for (var columnIndex = 0; columnIndex < valuePumps.length; columnIndex++) {
      final var columnSchemaNode = columnSchemaNodes.get(columnIndex);
      final var columnChunkReader =
          rowGroupReader
              .getColumnChunkReaderForSchemaPath(rowGroupRangeReader, columnSchemaNode)
              .orElseThrow();
      valuePumps[columnIndex] =
          ValuePump.create(
              columnChunkReader,
              rowGroupRangeReader,
              writer.getColumnChunkWriter(columnSchemaNode.getPath()),
              columnSchemaNode);
    }

    for (final var valuePump : valuePumps) {
      valuePump.transferRows(rowsToKeep);
    }

    writer.finishCurrentRowGroup(countOfRowsToKeep);
  }

  private record ValuePump(
      CompletableFuture<FlatColumnIterator> columnValueIteratorFuture,
      ColumnChunkWriter columnChunkWriter) {
    public static ValuePump create(
        final ColumnChunkReader columnChunkReader,
        final ByteRangeReader byteRangeReader,
        final ColumnChunkWriter columnChunkWriter,
        final ParquetSchemaNode columnSchemaNode) {
      return new ValuePump(
          columnChunkReader
              .readPages(byteRangeReader)
              .thenApplyAsync(
                  dataPageReaderIterator ->
                      new FlatColumnIterator(dataPageReaderIterator, columnSchemaNode)),
          columnChunkWriter);
    }

    abstract static class ColumnWritingVisitor implements Values.Visitor {
      private final ColumnChunkWriter columnChunkWriter;
      protected int repetitionLevel, definitionLevel;

      public ColumnWritingVisitor(ColumnChunkWriter columnChunkWriter) {
        this.columnChunkWriter = columnChunkWriter;
      }

      public void visitNull(int pageIndex) {
        columnChunkWriter.accumulateNull(repetitionLevel, definitionLevel);
      }

      public void setRepetitionLevel(final int repetitionLevel) {
        this.repetitionLevel = repetitionLevel;
      }

      public void setDefinitionLevel(final int definitionLevel) {
        this.definitionLevel = definitionLevel;
      }
    }

    public void transferRows(final BitSet rowsToKeep) {
      final var columnValueIterator = columnValueIteratorFuture.join();
      var rowIndex = 0;
      final ColumnWritingVisitor columnWritingVisitor =
          switch (columnChunkWriter.getColumnType().getType()) {
            case BOOLEAN ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(
                        repetitionLevel, values.getBoolean(valueIndex));
                  }
                };
            case INT32 ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(repetitionLevel, values.getInt32(valueIndex));
                  }
                };
            case INT64 ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(repetitionLevel, values.getInt64(valueIndex));
                  }
                };
            case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
            case FLOAT ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(repetitionLevel, values.getFloat(valueIndex));
                  }
                };
            case DOUBLE ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(
                        repetitionLevel, values.getDouble(valueIndex));
                  }
                };
            case BYTE_ARRAY ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(
                        repetitionLevel, values.getByteBuffer(valueIndex));
                  }
                };
            case FIXED_LEN_BYTE_ARRAY ->
                new ColumnWritingVisitor(columnChunkWriter) {
                  @Override
                  public void visit(
                      final int pageIndex, final Values values, final int valueIndex) {
                    columnChunkWriter.accumulateValue(
                        repetitionLevel, values.getByteBuffer(valueIndex));
                  }
                };
          };
      while (columnValueIterator.hasNext()) {
        if (rowsToKeep.get(rowIndex++)) {
          do {
            columnWritingVisitor.setRepetitionLevel(columnValueIterator.peekRepetitionLevel());
            columnWritingVisitor.setDefinitionLevel(columnValueIterator.peekDefinitionLevel());
            columnValueIterator.visitNext(columnWritingVisitor);
          } while (columnValueIterator.hasNext()
              && columnValueIterator.peekRepetitionLevel() > 0); // zero means a new row
        } else {
          do {
            columnValueIterator.visitNext(Values.NoOpVisitor.INSTANCE);
          } while (columnValueIterator.hasNext()
              && columnValueIterator.peekRepetitionLevel() > 0); // zero means a new row
        }
      }
    }
  }

  private static void copyRowGroup(
      final RowGroup rowGroup,
      final ByteRangeReader sourceRangeReader,
      final RowGroupWriter<Object> writer)
      throws IOException {
    writer.injectForeignRowGroup(rowGroup, sourceRangeReader);
  }
}
