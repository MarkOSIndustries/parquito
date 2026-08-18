package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquetFooter.PARQUET_UNENCRYPTED_MAGIC_BYTES;
import static java.util.stream.Collectors.toMap;

import com.markosindustries.parquito.encoding.LittleEndian;
import com.markosindustries.parquito.rows.RowAccumulator;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;

public class RowGroupWriter<Row> implements AutoCloseable {
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
  private final ByteBufferOutputStream bloomOutputStream;
  private final WriteSpec writeSpec;
  private final FileMetaData fileMetaData;
  private final Map<String, String> userMetadata;
  private final ParquetSchemaNode.Root schemaRoot;
  private final ArrayList<SortingColumn> sortingColumns;
  private final ArrayList<ColumnChunkWriter> columnChunkWriters;
  private final Map<ParquetSchemaPath, ColumnChunkWriter> columnChunkWritersByPath;
  private final RowAccumulator<Row> rowAccumulator;
  private RowGroup currentRowGroup;

  public RowGroupWriter(
      final OutputStream outputStream, final WriteSpec writeSpec, final Writer<Row> writer)
      throws IOException {
    this.byteCountingStream = new ByteCountingOutputStream(outputStream);
    byteCountingStream.write(PARQUET_UNENCRYPTED_MAGIC_BYTES);
    this.bloomOutputStream = new ByteBufferOutputStream();
    this.writeSpec = writeSpec;
    this.fileMetaData = new FileMetaData();
    this.fileMetaData.setSchema(List.copyOf(writer.getRawSchema()));
    this.fileMetaData.setCreated_by(CREATED_BY_STRING);
    this.fileMetaData.setVersion(2);
    this.fileMetaData.setNum_rows(0);
    this.userMetadata = new HashMap<>();
    this.schemaRoot = writer.getSchemaRoot();

    // TODO - make this configurable
    this.sortingColumns = new ArrayList<SortingColumn>(schemaRoot.getLeafCount());
    for (var i = 0; i < schemaRoot.getLeafCount(); i++) {
      sortingColumns.add(i, new SortingColumn(i, false, true));
    }

    this.currentRowGroup = newRowGroup();
    this.columnChunkWriters =
        buildColumnChunkWritersRecursively(
            new ArrayList<ColumnChunkWriter>(schemaRoot.getLeafCount()), schemaRoot);
    this.columnChunkWritersByPath =
        columnChunkWriters.stream()
            .collect(
                toMap(
                    columnChunkWriter -> columnChunkWriter.getColumnType().schemaNode().getPath(),
                    Function.identity()));

    this.rowAccumulator = new RowAccumulator<>(schemaRoot, writer.getTranslator(), this);
  }

  public void write(final Iterable<? extends Row> rows) throws IOException {
    write(rows.iterator());
  }

  public void write(final Iterator<? extends Row> rows) throws IOException {
    while (rows.hasNext()) {
      write(rows.next());
    }
  }

  public void write(final Row row) throws IOException {
    if (rowAccumulator.estimatedBytesRequired() >= writeSpec.targetBytesPerRowGroup()
        || currentRowGroup.num_rows >= writeSpec.maxRowsPerRowGroup()) {
      finishCurrentRowGroup(currentRowGroup.num_rows);
      rowAccumulator.resetEstimatedBytesRequired();
    }
    rowAccumulator.accumulate(row);
    currentRowGroup.num_rows++;
  }

  public void putMetaData(final String key, final String value) {
    userMetadata.put(key, value);
  }

  /**
   * Exists only for the ParquetRewriter to be able to bit-copy whole row groups
   *
   * @param foreignRowGroup The header info for the row group
   * @param byteRangeReader The byteRangeReader from which we can read the compressed row group and
   *     other referenced blobs
   */
  void injectForeignRowGroup(final RowGroup foreignRowGroup, final ByteRangeReader byteRangeReader)
      throws IOException {
    // We need to be on a row group boundary or something has gone wrong
    assert currentRowGroup.num_rows == 0;

    final var alterredRowGroup = foreignRowGroup.deepCopy();
    alterredRowGroup.setFile_offset(byteCountingStream.getBytesWritten());

    byteRangeReader.transferTo(
        foreignRowGroup.file_offset,
        (int) foreignRowGroup.total_compressed_size,
        Channels.newChannel(byteCountingStream));

    final var offsetShift = alterredRowGroup.file_offset - foreignRowGroup.file_offset;
    for (final var column : alterredRowGroup.columns) {
      // Despite file_offset being deprecated in parquet-format (see PARQUET-2139), it should still
      // be kept in sync with the data page offset otherwise a chunk may become unreadable by a
      // reader relying on file_offset
      column.setFile_offset(column.file_offset + offsetShift);
      if (column.meta_data.isSetData_page_offset()) {
        column.meta_data.data_page_offset += offsetShift;
      }
      if (column.meta_data.isSetDictionary_page_offset()) {
        column.meta_data.dictionary_page_offset += offsetShift;
      }
      if (column.meta_data.isSetBloom_filter_offset()) {
        final var bloomFilter =
            ColumnChunkReader.readBloomFilter(byteRangeReader, column.meta_data).join();
        column.meta_data.bloom_filter_offset = bloomOutputStream.size();
        ColumnChunkWriter.writeBloomFilter(bloomFilter, bloomOutputStream);
      }
    }

    fileMetaData.addToRow_groups(alterredRowGroup);
    fileMetaData.num_rows += alterredRowGroup.num_rows;
  }

  void finishCurrentRowGroup(final long numRowsInGroup) throws IOException {
    currentRowGroup.setFile_offset(byteCountingStream.getBytesWritten());
    currentRowGroup.setTotal_byte_size(0);
    currentRowGroup.setTotal_compressed_size(0);
    for (final var columnChunkWriter : columnChunkWriters) {
      final var offset = byteCountingStream.getBytesWritten();
      final var bloomOffset = bloomOutputStream.size();
      final var columnChunkHeader =
          columnChunkWriter.writeAllAndReset(byteCountingStream, bloomOutputStream);
      if (columnChunkHeader.meta_data.isSetDictionary_page_offset()) {
        columnChunkHeader.meta_data.dictionary_page_offset += offset;
      }
      columnChunkHeader.meta_data.data_page_offset += offset;
      columnChunkHeader.setFile_offset(columnChunkHeader.meta_data.data_page_offset);

      if (columnChunkHeader.meta_data.isSetBloom_filter_offset()) {
        columnChunkHeader.meta_data.bloom_filter_offset += bloomOffset;
      }

      currentRowGroup.total_byte_size += columnChunkHeader.meta_data.total_uncompressed_size;
      currentRowGroup.total_compressed_size += columnChunkHeader.meta_data.total_compressed_size;
      currentRowGroup.addToColumns(columnChunkHeader);
    }

    currentRowGroup.setNum_rows(numRowsInGroup);

    fileMetaData.addToRow_groups(currentRowGroup);
    fileMetaData.num_rows += currentRowGroup.num_rows;

    currentRowGroup = newRowGroup();
  }

  private RowGroup newRowGroup() {
    final var rowGroup = new RowGroup();
    rowGroup.setSorting_columns(sortingColumns);
    rowGroup.setTotal_byte_size(0);
    rowGroup.setNum_rows(0);
    return rowGroup;
  }

  private ArrayList<ColumnChunkWriter> buildColumnChunkWritersRecursively(
      final ArrayList<ColumnChunkWriter> columnChunkWriters, final ParquetSchemaNode schemaNode) {
    // It's important we visit nodes in the same order as the schema elements in fileMetaData.schema
    // - so depth first

    if (schemaNode.getColumnIndex().isPresent()) {
      if (schemaNode.getElement().type == null) {
        throw new UnsupportedOperationException(
            "Can't write schemas with empty groups - " + schemaNode.getPath());
      }
      final var columnMetadata =
          new ColumnMetaData(
              schemaNode.getElement().type,
              null,
              schemaNode.getPath().asNamesOnly(),
              writeSpec.compressionCodec(),
              0,
              0,
              0,
              0);
      final var columnType =
          ColumnType.create(sortingColumns.get(schemaNode.getColumnIndex().getAsInt()), schemaNode);
      columnChunkWriters.add(ColumnChunkWriter.create(columnMetadata, columnType, writeSpec));
    } else {
      for (final var child : schemaNode.getChildren()) {
        buildColumnChunkWritersRecursively(columnChunkWriters, child);
      }
    }

    return columnChunkWriters;
  }

  @Override
  public void close() throws Exception {
    if (currentRowGroup.num_rows > 0 || fileMetaData.num_rows == 0) {
      finishCurrentRowGroup(currentRowGroup.num_rows);
    }

    final var bytesBeforeBloomFilters = byteCountingStream.getBytesWritten();
    bloomOutputStream.writeTo(byteCountingStream);
    for (final var rowGroup : fileMetaData.row_groups) {
      for (final var column : rowGroup.columns) {
        if (column.meta_data.isSetBloom_filter_offset()) {
          column.meta_data.bloom_filter_offset += bytesBeforeBloomFilters;
        }
      }
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
    LittleEndian.writeInt((int) footerBytes, byteCountingStream);
    byteCountingStream.write(PARQUET_UNENCRYPTED_MAGIC_BYTES);
    byteCountingStream.flush();
  }

  public ColumnChunkWriter getColumnChunkWriter(final ParquetSchemaPath parquetSchemaPath) {
    return columnChunkWritersByPath.get(parquetSchemaPath);
  }
}
