package com.markosindustries.parquito;

import static com.markosindustries.parquito.encoding.IntEncodings.INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER;

import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.types.ColumnType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;
import java.util.SortedSet;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;

public class ColumnChunkWriter<ReadAs> {
  private final ColumnMetaData columnMetaData;
  private final ColumnType<ReadAs> columnType;
  private final int leafDefinitionLevel;
  private final int leafRepetitionLevel;

  private final ArrayList<ReadAs> values;
  private final IntArrayList definitionLevels;
  private final IntArrayList repetitionLevels;
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
    this.values = new ArrayList<>();
    this.definitionLevels = new IntArrayList();
    this.repetitionLevels = new IntArrayList();
    startNewChunk();
  }

  public static <ReadAs> ColumnChunkWriter<ReadAs> create(
      final ColumnMetaData columnMetaData, final ColumnType<ReadAs> columnType) {
    return new ColumnChunkWriter<ReadAs>(columnMetaData, columnType);
  }

  private void startNewChunk() {
    this.values.clear();
    this.definitionLevels.clear();
    this.repetitionLevels.clear();
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
    this.header.meta_data.num_values++;
    this.header.meta_data.statistics.null_count++;
    this.header.meta_data.statistics.setNull_countIsSet(true);

    this.definitionLevels.add(parquetSchemaNode.getDefinitionLevelMax());
    this.repetitionLevels.add(groupRepetitionLevel);
    groupRepetitionLevel = parquetSchemaNode.getRepetitionLevelMax();
  }

  public void accumulateValue(final ReadAs value) {
    this.header.meta_data.num_values++;
    this.values.add(Objects.requireNonNull(value));
    this.definitionLevels.add(leafDefinitionLevel);
    this.repetitionLevels.add(groupRepetitionLevel);
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

    // TODO refactor and split this stuff into DataPageWriter type classes
    //  Write a data page with definition levels, repetition levels, and values in whatever encoding
    final var pageOutputBufferStream = new ByteBufferOutputStream();

    // TODO - drive this off the schema node type and maybe something about value cardinality,
    // ordering, etc
    final Encoding selectedEncoding = Encoding.PLAIN;

    final var compressedPageOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedPageOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(header.meta_data.codec, compressedPageOutputStream));

    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        repetitionLevels,
        IntEncodings.bitWidth(getColumnType().schemaNode().getRepetitionLevelMax()),
        uncompressedPageOutputStream);
    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        definitionLevels,
        IntEncodings.bitWidth(getColumnType().schemaNode().getDefinitionLevelMax()),
        uncompressedPageOutputStream);

    Encodings.<ReadAs>getEncoding(header.meta_data.encodings.getFirst())
        .encode(values, uncompressedPageOutputStream, this);

    final var pageHeader =
        new PageHeader(
            PageType.DATA_PAGE_V2,
            uncompressedPageOutputStream.getBytesWritten(),
            compressedPageOutputStream.getBytesWritten());
    // TODO - we could look at counting rows... seems expensive with current structure though
    // TODO - separate pages from column chunks - allow multiple smaller pages
    pageHeader.data_page_header_v2 =
        new DataPageHeaderV2()
            .setNum_values((int) header.meta_data.num_values)
            .setNum_nulls((int) header.meta_data.statistics.null_count)
            .setIs_compressed(!header.meta_data.codec.equals(CompressionCodec.UNCOMPRESSED))
            .setEncoding(selectedEncoding);

    // We need to count the uncompressed bytes written for the page header
    final var outputCountingStream = new ByteCountingOutputStream(outputStream);
    Util.writePageHeader(pageHeader, outputCountingStream);
    header.meta_data.setTotal_compressed_size(
        outputCountingStream.getBytesWritten() + pageHeader.compressed_page_size);
    header.meta_data.setTotal_uncompressed_size(
        outputCountingStream.getBytesWritten() + pageHeader.uncompressed_page_size);
    pageOutputBufferStream.writeTo(outputStream);

    final var result = header;
    header = makeHeader();
    startNewChunk();
    return result;
  }

  private ColumnChunk makeHeader() {
    return new ColumnChunk()
        .setMeta_data(columnMetaData.deepCopy().setNum_values(0).setStatistics(new Statistics()));
  }
}
