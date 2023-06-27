package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.OptionalBranchIterator;
import com.markosindustries.parquito.rows.OptionalValueIterator;
import com.markosindustries.parquito.rows.ParquetFieldIterator;
import com.markosindustries.parquito.rows.RepeatedBranchIterator;
import com.markosindustries.parquito.rows.RepeatedValueIterator;
import com.markosindustries.parquito.types.ColumnType;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.RowGroup;

public class RowGroupReader {
  private final RowGroup rowGroupHeader;

  public RowGroupReader(RowGroup rowGroupHeader) {
    this.rowGroupHeader = rowGroupHeader;
  }

  public RowGroup getHeader() {
    return rowGroupHeader;
  }

  public <Repeated, Value> Iterator<Value> getRowIterator(
      final Reader<Repeated, Value> reader,
      final ParquetSchemaNode.Root parquetSchemaRoot,
      final ByteRangeReader byteRangeReader) {
    return new OptionalBranchIterator<>(
        parquetSchemaRoot.getChildren().stream()
            .collect(
                Collectors.toMap(
                    child -> child,
                    child -> {
                      return getRowIterator(
                          reader.getChild(child),
                          parquetSchemaRoot.getChild(child),
                          byteRangeReader);
                    })),
        parquetSchemaRoot,
        reader);
  }

  public <Repeated, Value> ParquetFieldIterator<?> getRowIterator(
      final Reader<Repeated, Value> reader,
      final ParquetSchemaNode parquetSchema,
      final ByteRangeReader byteRangeReader) {
    final var maybeColumnChunkHeader = getColumnChunkHeaderForSchemaPath(parquetSchema.getPath());
    if (maybeColumnChunkHeader.isPresent()) {
      final var columnChunkHeader = maybeColumnChunkHeader.get();
      final var columnType = ColumnType.create(columnChunkHeader, parquetSchema);
      return iterateLeaf(reader, parquetSchema, columnChunkHeader, columnType, byteRangeReader);
    } else {
      return iterateBranch(reader, parquetSchema, byteRangeReader);
    }
  }

  private <ReadAs extends Comparable<ReadAs>, Repeated, Value> ParquetFieldIterator<?> iterateLeaf(
      final Reader<Repeated, Value> reader,
      final ParquetSchemaNode parquetSchema,
      final org.apache.parquet.format.ColumnChunk columnChunkHeader,
      final ColumnType<ReadAs> columnType,
      final ByteRangeReader byteRangeReader) {
    var columnChunk =
        ColumnChunkReader.create(
            columnChunkHeader, columnType, byteRangeReader);
    final var dataPageIterator = columnChunk.readPages(byteRangeReader).join();
    return switch (parquetSchema.getRepetitionType()) {
      case REQUIRED, OPTIONAL -> {
        // Required can be nested within Optional/Repeated, so we always have to respect definition
        // levels
        yield new OptionalValueIterator<>(dataPageIterator, parquetSchema);
      }
      case REPEATED -> {
        yield new RepeatedValueIterator<>(dataPageIterator, parquetSchema, reader);
      }
    };
  }

  private <Repeated, Value> ParquetFieldIterator<?> iterateBranch(
      final Reader<Repeated, Value> reader,
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
                .collect(
                    Collectors.toMap(
                        child -> child,
                        child -> {
                          return getRowIterator(
                              reader.getChild(child),
                              parquetSchema.getChild(child),
                              byteRangeReader);
                        })),
            parquetSchema,
            reader);
      }
      case REPEATED -> {
        yield new RepeatedBranchIterator<>(
            parquetSchema.getChildren().stream()
                .collect(
                    Collectors.toMap(
                        child -> child,
                        child -> {
                          return getRowIterator(
                              reader.getChild(child),
                              parquetSchema.getChild(child),
                              byteRangeReader);
                        })),
            parquetSchema,
            reader);
      }
    };
  }

  public Optional<ColumnChunk> getColumnChunkHeaderForSchemaPath(String... schemaPath) {
    var matchingChunks =
        rowGroupHeader.columns.stream()
            .filter(
                columnChunk -> columnChunk.meta_data.path_in_schema.size() == schemaPath.length);
    for (int i = 0; i < schemaPath.length; i++) {
      final var pathElementIndex = i;
      matchingChunks =
          matchingChunks.filter(
              columnChunk ->
                  columnChunk
                      .meta_data
                      .path_in_schema
                      .get(pathElementIndex)
                      .equals(schemaPath[pathElementIndex]));
    }
    return matchingChunks.findAny();
  }

  public List<ColumnChunk> getColumnChunkHeadersUnderSchemaPath(String... schemaPath) {
    var matchingChunks =
        rowGroupHeader.columns.stream()
            .filter(
                columnChunk -> columnChunk.meta_data.path_in_schema.size() >= schemaPath.length);
    for (int i = 0; i < schemaPath.length; i++) {
      final var pathElementIndex = i;
      matchingChunks =
          matchingChunks.filter(
              columnChunk ->
                  columnChunk
                      .meta_data
                      .path_in_schema
                      .get(pathElementIndex)
                      .equals(schemaPath[pathElementIndex]));
    }
    return matchingChunks.toList();
  }
}
