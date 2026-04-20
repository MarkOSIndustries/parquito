package com.markosindustries.parquito;

import com.markosindustries.parquito.page.DataPageWriter;
import com.markosindustries.parquito.types.ColumnType;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.SortedSet;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;

public class ColumnChunkWriter<ReadAs> {
  private final ColumnMetaData columnMetaData;
  private final ColumnType<ReadAs> columnType;
  private final int leafDefinitionLevel;
  private final int leafRepetitionLevel;

  private DataPageWriter<ReadAs> dataPageWriter;
  private int groupDefinitionLevel;
  private int groupRepetitionLevel;
  private ColumnChunk header;
  private ReadAs minValue;
  private ReadAs maxValue;

  // TODO - naively assuming it'll make sense for this guy to hold onto the header and perform
  // mutations
  public ColumnChunkWriter(
      final ColumnMetaData columnMetaData, final ColumnType<ReadAs> columnType) {
    this.columnMetaData = columnMetaData;
    this.columnType = columnType;
    this.leafDefinitionLevel = columnType.schemaNode().getDefinitionLevelMax();
    this.leafRepetitionLevel = columnType.schemaNode().getRepetitionLevelMax();
    startNewChunk();
  }

  public static <ReadAs> ColumnChunkWriter<ReadAs> create(
      final ColumnMetaData columnMetaData, final ColumnType<ReadAs> columnType) {
    return new ColumnChunkWriter<ReadAs>(columnMetaData, columnType);
  }

  private void startNewChunk() {
    this.dataPageWriter = DataPageWriter.create(this, PageType.DATA_PAGE_V2);
    this.groupDefinitionLevel = 0;
    this.groupRepetitionLevel = 0;
    this.header = makeHeader();
  }

  public ColumnType<ReadAs> getColumnType() {
    return columnType;
  }

  public void writeDictionaryPage(final SortedSet<ReadAs> dictionaryValues) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public int getRequiredBytesToWrite(final ReadAs value) {
    return columnType.parquetType().getRequiredBytesToWrite(value);
  }

  // Call as we're going down the schema if node is non-leaf
  public void enterGroup(final ParquetSchemaNode parquetSchemaNode) {
    groupDefinitionLevel = parquetSchemaNode.getDefinitionLevelMax();
  }

  // Call as we're going back up the schema if node is non-leaf
  public void leaveGroup(final ParquetSchemaNode parquetSchemaNode) {
    groupRepetitionLevel = parquetSchemaNode.getRepetitionLevelMax();
  }

  public void accumulateNull(final ParquetSchemaNode parquetSchemaNode) {
    dataPageWriter.addNull(groupRepetitionLevel, parquetSchemaNode.getDefinitionLevelMax());
    groupRepetitionLevel = parquetSchemaNode.getRepetitionLevelMax();
  }

  public void accumulateValue(final ReadAs value) {
    dataPageWriter.addValue(
        Objects.requireNonNull(value), groupRepetitionLevel, leafDefinitionLevel);
    groupRepetitionLevel = leafRepetitionLevel;

    if (minValue == null || columnType.compare(minValue, value) > 0) {
      minValue = value;
    }
    if (maxValue == null || columnType.compare(maxValue, value) < 0) {
      maxValue = value;
    }
  }

  public ColumnChunk writeAllAndReset(final OutputStream outputStream) throws IOException {
    if (minValue != null) {
      final var minBuffer =
          ByteBuffer.allocate(columnType.parquetType().getRequiredBytesToWrite(minValue));
      columnType.parquetType().writeToByteBuffer(minValue, minBuffer);
      this.header.meta_data.statistics.setMin_value(minBuffer);
    }
    if (maxValue != null) {
      final var maxBuffer =
          ByteBuffer.allocate(columnType.parquetType().getRequiredBytesToWrite(maxValue));
      columnType.parquetType().writeToByteBuffer(maxValue, maxBuffer);
      this.header.meta_data.statistics.setMax_value(maxBuffer);
    }

    final var countingOutputStream = new ByteCountingOutputStream(outputStream);
    final var pageHeader = dataPageWriter.writePage(this.header.meta_data, countingOutputStream);
    this.header.meta_data.total_compressed_size += countingOutputStream.getBytesWritten();
    this.header.meta_data.total_uncompressed_size +=
        countingOutputStream.getBytesWritten()
            + (pageHeader.uncompressed_page_size - pageHeader.compressed_page_size);
    this.header.meta_data.num_values += dataPageWriter.getNumValues(pageHeader);
    this.header.meta_data.statistics.null_count += dataPageWriter.getNumNulls(pageHeader);

    final var result = header;
    header = makeHeader();
    startNewChunk();
    return result;
  }

  private ColumnChunk makeHeader() {
    return new ColumnChunk()
        .setMeta_data(
            columnMetaData
                .deepCopy()
                .setTotal_compressed_size(0)
                .setTotal_uncompressed_size(0)
                .setNum_values(0)
                .setStatistics(new Statistics().setNull_count(0)));
  }
}
