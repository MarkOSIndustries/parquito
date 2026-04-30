package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.page.DataPageWriter;
import com.markosindustries.parquito.page.DictionaryPageWriter;
import com.markosindustries.parquito.types.ColumnType;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Objects;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;

public class ColumnChunkWriter<Value> {
  private final ColumnMetaData columnMetaData;
  private final ColumnType<Value> columnType;
  private final int leafDefinitionLevel;
  private final int leafRepetitionLevel;
  private final EncodingSelector encodingSelector;
  private final BloomFilterSelector bloomFilterSelector;

  private DictionaryPageWriter<Value> dictionaryPageWriter;
  private DataPageWriter<Value> dataPageWriter;
  private ColumnChunk currentHeader;

  public ColumnChunkWriter(
      final ColumnMetaData columnMetaData,
      final ColumnType<Value> columnType,
      final WriteSpec writeSpec) {
    this.columnMetaData = columnMetaData;
    this.columnType = columnType;
    this.leafDefinitionLevel = columnType.schemaNode().getDefinitionLevelMax();
    this.leafRepetitionLevel = columnType.schemaNode().getRepetitionLevelMax();
    this.encodingSelector = writeSpec.encodingSelector();
    this.bloomFilterSelector = writeSpec.bloomFilterSelector();
    //    this.usesBloomFilter = true; // TODO - get from writer config
    startNewChunk();
  }

  public static <ReadAs> ColumnChunkWriter<ReadAs> create(
      final ColumnMetaData columnMetaData,
      final ColumnType<ReadAs> columnType,
      final WriteSpec writeSpec) {
    return new ColumnChunkWriter<ReadAs>(columnMetaData, columnType, writeSpec);
  }

  private void startNewChunk() {
    this.dictionaryPageWriter = new DictionaryPageWriter<>(this);
    this.dataPageWriter = DataPageWriter.create(this, PageType.DATA_PAGE_V2);
    this.currentHeader = makeHeader();
  }

  public ColumnType<Value> getColumnType() {
    return columnType;
  }

  public DictionaryPageWriter<Value> getDictionaryPageWriter() {
    return dictionaryPageWriter;
  }

  public void accumulateNull(final int repetitionLevel, final int definitionLevel) {
    dataPageWriter.addNull(repetitionLevel, definitionLevel);
  }

  public void accumulateValue(final int repetitionLevel, final Value value) {
    final var distinctValue = dictionaryPageWriter.addValue(Objects.requireNonNull(value));
    dataPageWriter.addValue(distinctValue, repetitionLevel, leafDefinitionLevel);
  }

  public ColumnChunk writeAllAndReset(final OutputStream outputStream) throws IOException {
    if (dictionaryPageWriter.getNumValues() > 0) {
      final var minValue = dictionaryPageWriter.getDistinctValues().first();
      final var maxValue = dictionaryPageWriter.getDistinctValues().first();

      final var minBuffer =
          ByteBuffer.allocate(columnType.parquetType().getRequiredBytesToWrite(minValue));
      columnType.parquetType().writeToByteBuffer(minValue, minBuffer);
      this.currentHeader.meta_data.statistics.setMin_value(minBuffer);

      final var maxBuffer =
          ByteBuffer.allocate(columnType.parquetType().getRequiredBytesToWrite(maxValue));
      columnType.parquetType().writeToByteBuffer(maxValue, maxBuffer);
      this.currentHeader.meta_data.statistics.setMax_value(maxBuffer);
    }

    final var selectedEncoding =
        (dataPageWriter.getNumValues() == dataPageWriter.getNumNulls())
            ? Encoding.PLAIN
            : encodingSelector.selectEncoding(
                this.columnMetaData,
                dictionaryPageWriter.getNumValues(),
                dataPageWriter.getNumValues(),
                dataPageWriter.getNumNulls());
    currentHeader.meta_data.addToEncodings(selectedEncoding);

    if (selectedEncoding == Encoding.RLE_DICTIONARY) {
      final var dictionaryOutputStream = new ByteCountingOutputStream(outputStream);
      final var pageHeader =
          dictionaryPageWriter.writePage(Encoding.PLAIN, columnMetaData, dictionaryOutputStream);
      this.currentHeader.meta_data.total_compressed_size +=
          dictionaryOutputStream.getBytesWritten();
      this.currentHeader.meta_data.total_uncompressed_size +=
          dictionaryOutputStream.getBytesWritten()
              + (pageHeader.uncompressed_page_size - pageHeader.compressed_page_size);
      this.currentHeader.meta_data.setDictionary_page_offset(0);
      this.currentHeader.meta_data.setData_page_offset(dictionaryOutputStream.getBytesWritten());
    } else {
      this.currentHeader.meta_data.setData_page_offset(0);
    }

    final var dataPageOutputStream = new ByteCountingOutputStream(outputStream);
    final var pageHeader =
        dataPageWriter.writePage(
            selectedEncoding, this.currentHeader.meta_data, dataPageOutputStream);
    this.currentHeader.meta_data.total_compressed_size += dataPageOutputStream.getBytesWritten();
    this.currentHeader.meta_data.total_uncompressed_size +=
        dataPageOutputStream.getBytesWritten()
            + (pageHeader.uncompressed_page_size - pageHeader.compressed_page_size);
    this.currentHeader.meta_data.num_values += dataPageWriter.getNumValues(pageHeader);
    this.currentHeader.meta_data.statistics.null_count += dataPageWriter.getNumNulls(pageHeader);

    if (bloomFilterSelector.shouldWriteBloomFilter(
        columnMetaData,
        dictionaryPageWriter.getNumValues(),
        dataPageWriter.getNumValues(),
        dataPageWriter.getNumNulls())) {
      final var bloomFilter = BloomFilter.create(dictionaryPageWriter.getDistinctValues(), 0.00001);
      writeBloomFilter(bloomFilter, outputStream);
      this.currentHeader.meta_data.setBloom_filter_offset(
          this.currentHeader.meta_data.data_page_offset + dataPageOutputStream.getBytesWritten());
    }

    final var result = currentHeader;
    currentHeader = makeHeader();
    startNewChunk();
    return result;
  }

  public static void writeBloomFilter(
      final BloomFilter bloomFilter, final OutputStream outputStream) throws IOException {
    Util.writeBloomFilterHeader(bloomFilter.header(), outputStream);
    if (bloomFilter.bitset().hasArray()) {
      outputStream.write(bloomFilter.bitset().array());
    } else {
      Channels.newChannel(outputStream).write(bloomFilter.bitset());
    }
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
