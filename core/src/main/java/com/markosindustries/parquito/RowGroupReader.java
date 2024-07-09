package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.OptionalBranchIterator;
import com.markosindustries.parquito.rows.OptionalValueIterator;
import com.markosindustries.parquito.rows.ParquetFieldIterator;
import com.markosindustries.parquito.rows.RepeatedBranchIterator;
import com.markosindustries.parquito.rows.RepeatedValueIterator;
import com.markosindustries.parquito.rows.RowIterator;
import com.markosindustries.parquito.types.ColumnType;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;

public record RowGroupReader(RowGroup rowGroupHeader, ParquetSchemaNode.Root schemaRoot) {

  public <Repeated, Value> Iterator<Value> getRowIterator(
      final RowReadSpec<Repeated, Value, ?> rowReadSpec, final ByteRangeReader byteRangeReader) {
    return new RowIterator<>(
        new OptionalBranchIterator<>(
            schemaRoot.getChildren().stream()
                .filter(rowReadSpec::includesChild)
                .collect(
                    Collectors.toMap(
                        child -> child,
                        child -> {
                          return iterateField(
                              rowReadSpec.forChild(child),
                              schemaRoot.getChild(child),
                              byteRangeReader);
                        })),
            schemaRoot,
            rowReadSpec));
  }

  private <ReadAs, Repeated, Value> ParquetFieldIterator<?> iterateField(
      final RowReadSpec<Repeated, Value, ReadAs> rowReadSpec,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var maybeColumnChunkReader =
        getColumnChunkReaderForSchemaPath(byteRangeReader, parquetSchema, parquetSchema.getPath());
    if (maybeColumnChunkReader.isPresent()) {
      return iterateLeaf(
          rowReadSpec,
          parquetSchema,
          (ColumnChunkReader<ReadAs>) maybeColumnChunkReader.get(),
          byteRangeReader);
    } else {
      return iterateBranch(rowReadSpec, parquetSchema, byteRangeReader);
    }
  }

  private <ReadAs, Repeated, Value> ParquetFieldIterator<?> iterateLeaf(
      final RowReadSpec<Repeated, Value, ReadAs> rowReadSpec,
      final ParquetSchemaNode parquetSchema,
      final ColumnChunkReader<ReadAs> columnChunkReader,
      final ByteRangeReader byteRangeReader) {
    final var dataPageIterator = columnChunkReader.readPages(byteRangeReader).join();
    return switch (parquetSchema.getRepetitionType()) {
      case REQUIRED, OPTIONAL -> {
        // Required can be nested within Optional/Repeated, so we always have to respect definition
        // levels
        yield new OptionalValueIterator<>(dataPageIterator, parquetSchema, rowReadSpec);
      }
      case REPEATED -> {
        yield new RepeatedValueIterator<>(dataPageIterator, parquetSchema, rowReadSpec);
      }
    };
  }

  private <Repeated, Value> ParquetFieldIterator<?> iterateBranch(
      final RowReadSpec<Repeated, Value, ?> rowReadSpec,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var repetitionType =
        parquetSchema.getRepetitionType() != null
            ? parquetSchema.getRepetitionType()
            : FieldRepetitionType.REQUIRED;
    return switch (repetitionType) {
      case REQUIRED, OPTIONAL -> {
        yield new OptionalBranchIterator<>(
            parquetSchema.getChildren().stream()
                .filter(rowReadSpec::includesChild)
                .collect(
                    Collectors.toMap(
                        child -> child,
                        child -> {
                          return iterateField(
                              rowReadSpec.forChild(child),
                              parquetSchema.getChild(child),
                              byteRangeReader);
                        })),
            parquetSchema,
            rowReadSpec);
      }
      case REPEATED -> {
        yield new RepeatedBranchIterator<>(
            parquetSchema.getChildren().stream()
                .filter(rowReadSpec::includesChild)
                .collect(
                    Collectors.toMap(
                        child -> child,
                        child -> {
                          return iterateField(
                              rowReadSpec.forChild(child),
                              parquetSchema.getChild(child),
                              byteRangeReader);
                        })),
            parquetSchema,
            rowReadSpec);
      }
    };
  }

  public Optional<? extends ColumnChunkReader<?>> getColumnChunkReaderForSchemaPath(
      final ByteRangeReader byteRangeReader, final String... schemaPath) {
    return getColumnChunkReaderForSchemaPath(
        byteRangeReader, schemaRoot.getChild(schemaPath), schemaPath);
  }

  private Optional<? extends ColumnChunkReader<?>> getColumnChunkReaderForSchemaPath(
      final ByteRangeReader byteRangeReader,
      final ParquetSchemaNode columnSchema,
      final String... schemaPath) {
    return getColumnChunkIndexForSchemaPath(schemaPath).stream()
        .mapToObj(
            columnChunkIndex ->
                ColumnChunkReader.create(
                    rowGroupHeader, columnChunkIndex, columnSchema, byteRangeReader))
        .findAny();
  }

  public OptionalInt getColumnChunkIndexForSchemaPath(final String... schemaPath) {
    var matchingIndices =
        IntStream.range(0, rowGroupHeader.columns.size())
            .filter(
                index ->
                    rowGroupHeader.columns.get(index).meta_data.path_in_schema.size()
                        == schemaPath.length);
    for (int i = 0; i < schemaPath.length; i++) {
      final var pathElementIndex = i;
      matchingIndices =
          matchingIndices.filter(
              index ->
                  rowGroupHeader
                      .columns
                      .get(index)
                      .meta_data
                      .path_in_schema
                      .get(pathElementIndex)
                      .equals(schemaPath[pathElementIndex]));
    }
    return matchingIndices.findAny();
  }

  public Optional<? extends ColumnType<?>> getColumnType(final String... schemaPath) {
    return getColumnChunkIndexForSchemaPath(schemaPath).stream()
        .mapToObj(
            columnChunkIndex -> {
              final var columnChunkHeader = rowGroupHeader.columns.get(columnChunkIndex);
              final var columnChunkSorting =
                  rowGroupHeader.isSetSorting_columns()
                      ? rowGroupHeader.sorting_columns.get(columnChunkIndex)
                      : new SortingColumn(columnChunkIndex, false, true);
              return ColumnType.create(
                  columnChunkHeader, columnChunkSorting, schemaRoot.getChild(schemaPath));
            })
        .findAny();
  }
}
