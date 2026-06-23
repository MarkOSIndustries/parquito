package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.page.DataPageWriter;
import com.markosindustries.parquito.page.DictionaryPageWriter;
import com.markosindustries.parquito.page.ValueAccumulator;
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

public class ColumnChunkWriter {
  private final ColumnMetaData columnMetaData;
  private final ColumnType columnType;
  private final int leafDefinitionLevel;
  private final int leafRepetitionLevel;
  private final EncodingSelector encodingSelector;
  private final BloomFilterSelector bloomFilterSelector;
  private final WriteSpec writeSpec;

  private ValueAccumulator valueAccumulator;
  private DataPageWriter dataPageWriter;
  private ColumnChunk currentHeader;

  public ColumnChunkWriter(
      final ColumnMetaData columnMetaData, final ColumnType columnType, final WriteSpec writeSpec) {
    this.columnMetaData = columnMetaData;
    this.columnType = columnType;
    this.leafDefinitionLevel = columnType.schemaNode().getDefinitionLevelMax();
    this.leafRepetitionLevel = columnType.schemaNode().getRepetitionLevelMax();
    this.encodingSelector = writeSpec.encodingSelector();
    this.bloomFilterSelector = writeSpec.bloomFilterSelector();
    this.writeSpec = writeSpec;
    startNewChunk();
  }

  public static ColumnChunkWriter create(
      final ColumnMetaData columnMetaData, final ColumnType columnType, final WriteSpec writeSpec) {
    return new ColumnChunkWriter(columnMetaData, columnType, writeSpec);
  }

  private void startNewChunk() {
    this.valueAccumulator = new ValueAccumulator(columnType);
    this.dataPageWriter = DataPageWriter.create(this, writeSpec, PageType.DATA_PAGE_V2);
    this.currentHeader = makeHeader();
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public void accumulateNull(final int repetitionLevel, final int definitionLevel) {
    dataPageWriter.addNull(repetitionLevel, definitionLevel);
  }

  public int accumulateValue(final int repetitionLevel, final boolean value) {
    final var bytes = valueAccumulator.addValue(value);
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public int accumulateValue(final int repetitionLevel, final ByteBuffer value) {
    final var bytes = valueAccumulator.addValue(Objects.requireNonNull(value));
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public int accumulateValue(final int repetitionLevel, final double value) {
    final var bytes = valueAccumulator.addValue(value);
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public int accumulateValue(final int repetitionLevel, final float value) {
    final var bytes = valueAccumulator.addValue(value);
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public int accumulateValue(final int repetitionLevel, final int value) {
    final var bytes = valueAccumulator.addValue(value);
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public int accumulateValue(final int repetitionLevel, final long value) {
    final var bytes = valueAccumulator.addValue(value);
    dataPageWriter.addValue(repetitionLevel, leafDefinitionLevel);
    return bytes;
  }

  public ColumnChunk writeAllAndReset(
      final OutputStream rowGroupOutputStream, final OutputStream bloomOutputStream)
      throws IOException {
    final var valuesReadyToWrite = valueAccumulator.makeReadyToWrite();

    if (valuesReadyToWrite.getNumDistinctValues() > 0) {
      this.currentHeader.meta_data.statistics.setMin_value(valuesReadyToWrite.getMinValue());
      this.currentHeader.meta_data.statistics.setMax_value(valuesReadyToWrite.getMaxValue());
    }

    final var selectedEncoding =
        (dataPageWriter.getNumValues() == dataPageWriter.getNumNulls())
            ? Encoding.PLAIN
            : encodingSelector.selectEncoding(
                this.columnMetaData.type,
                this.columnType.schemaNode().getPath(),
                valuesReadyToWrite.getNumDistinctValues(),
                dataPageWriter.getNumValues(),
                dataPageWriter.getNumNulls());
    currentHeader.meta_data.addToEncodings(selectedEncoding);

    if (selectedEncoding == Encoding.RLE_DICTIONARY) {
      final var dictionaryOutputStream = new ByteCountingOutputStream(rowGroupOutputStream);
      final var pageHeader =
          new DictionaryPageWriter()
              .writePage(
                  valuesReadyToWrite.sliceDistinctValues(), columnMetaData, dictionaryOutputStream);
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

    final var dataPageOutputStream = new ByteCountingOutputStream(rowGroupOutputStream);
    final var pageHeaders =
        dataPageWriter.writePages(
            valuesReadyToWrite.makeSlice(),
            valueAccumulator.getEstimatedBytesRequired(),
            selectedEncoding,
            this.currentHeader.meta_data,
            dataPageOutputStream);
    this.currentHeader.meta_data.num_values += dataPageWriter.getNumValues();
    this.currentHeader.meta_data.statistics.null_count += dataPageWriter.getNumNulls();
    this.currentHeader.meta_data.total_compressed_size += dataPageOutputStream.getBytesWritten();
    this.currentHeader.meta_data.total_uncompressed_size += dataPageOutputStream.getBytesWritten();
    for (final var pageHeader : pageHeaders) {
      this.currentHeader.meta_data.total_uncompressed_size +=
          (pageHeader.uncompressed_page_size - pageHeader.compressed_page_size);
    }

    final var bloomFilterSizeInBytes =
        bloomFilterSelector.shouldWriteBloomFilter(
            this.columnMetaData.type,
            this.columnType.schemaNode().getPath(),
            valueAccumulator.getNumValues(),
            dataPageWriter.getNumValues(),
            dataPageWriter.getNumNulls());
    if (bloomFilterSizeInBytes.isPresent()) {
      final var bloomFilter = BloomFilter.createEmpty(bloomFilterSizeInBytes.get());
      valuesReadyToWrite.fillBloomFilter(bloomFilter);
      writeBloomFilter(bloomFilter, bloomOutputStream);
      this.currentHeader.meta_data.setBloom_filter_offset(0);
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
