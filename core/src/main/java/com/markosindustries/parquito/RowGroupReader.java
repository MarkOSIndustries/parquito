package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.NoOpFieldIterator;
import com.markosindustries.parquito.rows.OptionalBranchIterator;
import com.markosindustries.parquito.rows.OptionalValueIterator;
import com.markosindustries.parquito.rows.ParquetFieldIterator;
import com.markosindustries.parquito.rows.PushdownPredicates;
import com.markosindustries.parquito.rows.RepeatedBranchIterator;
import com.markosindustries.parquito.rows.RepeatedValueIterator;
import com.markosindustries.parquito.rows.RowIterator;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import com.markosindustries.parquito.types.ConversionStrategy;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;

public record RowGroupReader(RowGroup rowGroupHeader, ParquetSchemaNode.Root schemaRoot) {
  public CompletableFuture<ByteRangeReader> preloadRowGroup(final ByteRangeReader byteRangeReader) {
    return byteRangeReader
        .readAsBuffer(rowGroupHeader().file_offset, (int) rowGroupHeader.total_compressed_size)
        .thenApplyAsync(
            entireRowGroupBuffer ->
                new TieredCompositeByteRangeReader(
                    new ByteBufferByteRangeReader(
                        entireRowGroupBuffer, rowGroupHeader().file_offset),
                    rowGroupHeader().file_offset,
                    rowGroupHeader().file_offset + rowGroupHeader.total_compressed_size,
                    byteRangeReader),
            Concurrency.DEFAULT_EXECUTOR);
  }

  public <Row> Iterator<Row> getRowIterator(
      final RowReadSpec<Row> rowReadSpec, final ByteRangeReader byteRangeReader) {
    final var schemaTraversalSpec =
        rowReadSpec
            .schemaTraversalSpec()
            .combineWith(rowReadSpec.predicate().asSchemaTraversalSpec());

    final var pushdownPredicates = new PushdownPredicates(rowReadSpec.predicate());

    final var parquetFieldIterators =
        makeFieldIterators(schemaTraversalSpec, pushdownPredicates, schemaRoot, byteRangeReader);

    return new RowIterator<>(
        pushdownPredicates,
        new OptionalBranchIterator(parquetFieldIterators, schemaRoot),
        rowReadSpec.reader());
  }

  private ParquetFieldIterator iterateField(
      final SchemaTraversalSpec schemaTraversalSpec,
      final PushdownPredicates pushdownPredicates,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var maybeColumnChunkReader =
        getColumnChunkReaderForSchemaPath(byteRangeReader, parquetSchema);
    if (maybeColumnChunkReader.isPresent()) {
      return iterateLeaf(
          pushdownPredicates, parquetSchema, maybeColumnChunkReader.get(), byteRangeReader);
    } else {
      return iterateBranch(schemaTraversalSpec, pushdownPredicates, parquetSchema, byteRangeReader);
    }
  }

  private ParquetFieldIterator iterateLeaf(
      final PushdownPredicates pushdownPredicates,
      final ParquetSchemaNode parquetSchema,
      final ColumnChunkReader columnChunkReader,
      final ByteRangeReader byteRangeReader) {
    final var dataPageIterator = columnChunkReader.readPages(byteRangeReader).join();
    return switch (parquetSchema.getRepetitionType()) {
      case REQUIRED, OPTIONAL -> {
        // Required can be nested within Optional/Repeated, so we always have to respect definition
        // levels
        yield new OptionalValueIterator(dataPageIterator, parquetSchema, pushdownPredicates);
      }
      case REPEATED -> {
        yield new RepeatedValueIterator(dataPageIterator, parquetSchema, pushdownPredicates);
      }
    };
  }

  private ParquetFieldIterator iterateBranch(
      final SchemaTraversalSpec schemaTraversalSpec,
      final PushdownPredicates pushdownPredicates,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var repetitionType =
        parquetSchema.getRepetitionType() != null
            ? parquetSchema.getRepetitionType()
            : FieldRepetitionType.REQUIRED;
    final var parquetFieldIterators =
        makeFieldIterators(schemaTraversalSpec, pushdownPredicates, parquetSchema, byteRangeReader);
    return switch (repetitionType) {
      case REQUIRED, OPTIONAL -> new OptionalBranchIterator(parquetFieldIterators, parquetSchema);
      case REPEATED -> new RepeatedBranchIterator(parquetFieldIterators, parquetSchema);
    };
  }

  ParquetFieldIterator[] makeFieldIterators(
      final SchemaTraversalSpec schemaTraversalSpec,
      final PushdownPredicates pushdownPredicates,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var iterators = new ParquetFieldIterator[parquetSchema.getChildren().length];
    for (var index = 0; index < parquetSchema.getChildren().length; index++) {
      iterators[index] =
          schemaTraversalSpec.includesChild(index)
              ? iterateField(
                  schemaTraversalSpec.forChild(index),
                  pushdownPredicates.forChild(index),
                  parquetSchema.getChildAtIndex(index),
                  byteRangeReader)
              : NoOpFieldIterator.INSTANCE;
    }
    return iterators;
  }

  public Optional<ColumnChunkReader> getColumnChunkReaderForSchemaPath(
      final ByteRangeReader byteRangeReader, final ParquetSchemaPath parquetSchemaPath) {
    return getColumnChunkReaderForSchemaPath(
        byteRangeReader, schemaRoot.getChild(parquetSchemaPath));
  }

  Optional<ColumnChunkReader> getColumnChunkReaderForSchemaPath(
      final ByteRangeReader byteRangeReader, final ParquetSchemaNode columnSchema) {
    //noinspection unchecked
    return columnSchema.getColumnIndex().stream()
        .mapToObj(
            columnChunkIndex ->
                ColumnChunkReader.create(
                    rowGroupHeader, columnChunkIndex, columnSchema, byteRangeReader))
        .findAny();
  }

  public OptionalInt getColumnChunkIndexForSchemaPath(final ParquetSchemaPath parquetSchemaPath) {
    return schemaRoot.getChild(parquetSchemaPath).getColumnIndex();
  }

  public Optional<? extends ConvertedColumnType<?>> getConvertedColumnType(
      final ParquetSchemaPath parquetSchemaPath, final ConversionStrategy conversionStrategy) {
    return getColumnType(parquetSchemaPath)
        .map(
            columnType ->
                new ConvertedColumnType<>(
                    columnType, conversionStrategy.converterFor(columnType.schemaNode())));
  }

  public Optional<ColumnType> getColumnType(final ParquetSchemaPath parquetSchemaPath) {
    return getColumnChunkIndexForSchemaPath(parquetSchemaPath).stream()
        .mapToObj(
            columnChunkIndex -> {
              final var columnChunkHeader = rowGroupHeader.columns.get(columnChunkIndex);
              final var columnChunkSorting =
                  rowGroupHeader.isSetSorting_columns()
                      ? rowGroupHeader.sorting_columns.get(columnChunkIndex)
                      : new SortingColumn(columnChunkIndex, false, true);
              return ColumnType.create(columnChunkSorting, schemaRoot.getChild(parquetSchemaPath));
            })
        .findAny();
  }
}
