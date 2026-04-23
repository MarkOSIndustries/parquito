package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquetFooter.PARQUET_UNENCRYPTED_MAGIC_BYTES;
import static java.util.stream.Collectors.toMap;

import com.markosindustries.parquito.encoding.LittleEndian;
import com.markosindustries.parquito.rows.RowAccumulator;
import com.markosindustries.parquito.types.ColumnType;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;

public class RowGroupWriter<Row> implements AutoCloseable, Writer.DataPageAccumulator {
  private static final String CREATED_BY_STRING;

  static {
    CREATED_BY_STRING =
        JarManifest.PARQUITO_GROUP
            + " version "
            + JarManifest.IMPLEMENTATION_VERSION
            + " build "
            + JarManifest.PARQUITO_COMMIT_SHA;
  }

  private final ByteCountingOutputStream byteCountingStream;
  private final WriteSpec writeSpec;
  private final FileMetaData fileMetaData;
  private final Map<String, String> userMetadata;
  private final ParquetSchemaNode.Root schemaRoot;
  private final ArrayList<SortingColumn> sortingColumns;
  private final ArrayList<ColumnChunkWriter<?>> columnChunkWriters;
  private final Map<ParquetSchemaPath, ColumnChunkWriter<?>> columnChunkWritersByPath;
  private final RowAccumulator<Row> rowAccumulator;
  private RowGroup currentRowGroup;

  public RowGroupWriter(
      final OutputStream outputStream, final WriteSpec writeSpec, final Writer<Row> writer)
      throws IOException {
    this.byteCountingStream = new ByteCountingOutputStream(outputStream);
    byteCountingStream.write(
        PARQUET_UNENCRYPTED_MAGIC_BYTES); // Spec says we also put the magic bytes at the start of
    // the file
    this.writeSpec = writeSpec;
    this.fileMetaData = new FileMetaData();
    this.fileMetaData.setSchema(List.copyOf(writer.getRawSchema()));
    this.fileMetaData.setCreated_by(CREATED_BY_STRING);
    this.fileMetaData.setVersion(2);
    this.userMetadata = new HashMap<>();
    this.schemaRoot = writer.getSchemaRoot();

    // TODO - make this configurable
    this.sortingColumns = new ArrayList<SortingColumn>(schemaRoot.getLeafCount());
    for (var i = 0; i < schemaRoot.getLeafCount(); i++) {
      sortingColumns.add(i, new SortingColumn(i, false, true));
    }

    this.currentRowGroup = newRowGroup();
    this.columnChunkWriters =
        buildColumnChunkWritersRecursively(new ArrayList<>(schemaRoot.getLeafCount()), schemaRoot);
    this.columnChunkWritersByPath =
        columnChunkWriters.stream()
            .collect(
                toMap(
                    columnChunkWriter -> columnChunkWriter.getColumnType().schemaNode().getPath(),
                    Function.identity()));

    this.rowAccumulator = new RowAccumulator<>(schemaRoot, writer.getTranslator(), this);
  }

  public void write(final Iterable<Row> rows) throws IOException {
    write(rows.iterator());
  }

  public void write(final Iterator<Row> rows) throws IOException {
    while (rows.hasNext()) {
      write(rows.next());
    }
  }

  public void write(final Row row) throws IOException {
    if (currentRowGroup.num_rows == writeSpec.maxRowsPerRowGroup()) {
      finishCurrentRowGroup();
      currentRowGroup = newRowGroup();
    }
    rowAccumulator.accumulate(row);
    currentRowGroup.num_rows++;
  }

  public void putMetaData(final String key, final String value) {
    userMetadata.put(key, value);
  }

  private void finishCurrentRowGroup() throws IOException {
    currentRowGroup.setFile_offset(byteCountingStream.getBytesWritten());
    currentRowGroup.setTotal_byte_size(0);
    currentRowGroup.setTotal_compressed_size(0);
    for (final var columnChunkWriter : columnChunkWriters) {
      final var offset = byteCountingStream.getBytesWritten();
      final var columnChunkHeader = columnChunkWriter.writeAllAndReset(byteCountingStream);
      columnChunkHeader.meta_data.setData_page_offset(offset);
      columnChunkHeader.setFile_offset(
          offset); // TODO - does this need to move after the dictionary gets written?
      // TODO remove this temp sanity check
      if (byteCountingStream.getBytesWritten() - offset
          != columnChunkHeader.meta_data.total_compressed_size) {
        throw new RuntimeException(
            "The compressed bytes counted in the RowGroupWriter didn't match what the ColumnChunkHeader counted");
      }
      currentRowGroup.total_byte_size += columnChunkHeader.meta_data.total_uncompressed_size;
      currentRowGroup.total_compressed_size += columnChunkHeader.meta_data.total_compressed_size;
      currentRowGroup.addToColumns(columnChunkHeader);
    }
    // TODO remove this temp sanity check
    if (byteCountingStream.getBytesWritten() - currentRowGroup.file_offset
        != currentRowGroup.total_compressed_size) {
      throw new RuntimeException(
          "The compressed bytes counted in the RowGroupWriter didn't match what the offset difference indicates");
    }

    fileMetaData.addToRow_groups(currentRowGroup);
  }

  private RowGroup newRowGroup() {
    final var rowGroup = new RowGroup();
    rowGroup.setSorting_columns(sortingColumns);
    rowGroup.setNum_rowsIsSet(true);
    return rowGroup;
  }

  private ArrayList<ColumnChunkWriter<?>> buildColumnChunkWritersRecursively(
      final ArrayList<ColumnChunkWriter<?>> columnChunkWriters,
      final ParquetSchemaNode schemaNode) {
    // It's important we visit nodes in the same order as the schema elements in fileMetaData.schema
    // - so depth first

    if (schemaNode.getColumnIndex().isPresent()) {
      final var columnMetadata =
          new ColumnMetaData(
              schemaNode.getElement().type,
              List.of(Encoding.PLAIN /* TODO */),
              schemaNode.getPath().asNamesOnly(),
              writeSpec.compressionCodec(),
              0,
              0,
              0,
              0);
      final var columnType =
          ColumnType.create(
              columnMetadata,
              sortingColumns.get(schemaNode.getColumnIndex().getAsInt()),
              schemaNode);
      columnChunkWriters.add(ColumnChunkWriter.create(columnMetadata, columnType));
    } else {
      for (final var child : schemaNode.getChildren()) {
        buildColumnChunkWritersRecursively(columnChunkWriters, child);
      }
    }

    return columnChunkWriters;
  }

  @Override
  public void close() throws Exception {
    if (currentRowGroup.num_rows > 0) {
      finishCurrentRowGroup();
    }
    fileMetaData.setKey_value_metadata(
        userMetadata.entrySet().stream()
            .map(
                e -> {
                  final var keyValue = new KeyValue(e.getKey());
                  keyValue.setValue(e.getValue());
                  return keyValue;
                })
            .toList());
    final var bytesBeforeFooter = byteCountingStream.getBytesWritten();
    ParquetFooter.write(fileMetaData, byteCountingStream).join();
    final var footerBytes = byteCountingStream.getBytesWritten() - bytesBeforeFooter;
    LittleEndian.writeInt(footerBytes, byteCountingStream);
    byteCountingStream.write(PARQUET_UNENCRYPTED_MAGIC_BYTES);
  }

  @Override
  public ColumnChunkWriter<?> getColumnChunkWriter(final ParquetSchemaPath parquetSchemaPath) {
    return columnChunkWritersByPath.get(parquetSchemaPath);
  }

  public record WriteSpec(long maxRowsPerRowGroup, CompressionCodec compressionCodec) {}
}
