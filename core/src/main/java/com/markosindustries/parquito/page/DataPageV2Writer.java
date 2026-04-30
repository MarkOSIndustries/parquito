package com.markosindustries.parquito.page;

import static com.markosindustries.parquito.encoding.IntEncodings.INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

public class DataPageV2Writer<Value> implements DataPageWriter<Value> {
  private final ColumnChunkWriter<Value> columnChunkWriter;
  private final DataPageHeaderV2 dataPageHeaderV2;
  private final ArrayList<Value> values;
  private final IntArrayList definitionLevels;
  private final IntArrayList repetitionLevels;

  public DataPageV2Writer(ColumnChunkWriter<Value> columnChunkWriter) {
    this.columnChunkWriter = columnChunkWriter;
    this.dataPageHeaderV2 = new DataPageHeaderV2().setNum_values(0).setNum_nulls(0).setNum_rows(0);
    this.values = new ArrayList<>();
    // TODO replace with RLE encoding on the fly to keep RAM low
    this.definitionLevels = new IntArrayList();
    this.repetitionLevels = new IntArrayList();
  }

  @Override
  public void addNull(final int repetitionLevel, final int definitionLevel) {
    dataPageHeaderV2.num_nulls++;
    dataPageHeaderV2.num_values++;
    if (repetitionLevel == 0) {
      dataPageHeaderV2.num_rows++;
    }
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public void addValue(final Value value, final int repetitionLevel, final int definitionLevel) {
    dataPageHeaderV2.num_values++;
    if (repetitionLevel == 0) {
      dataPageHeaderV2.num_rows++;
    }
    values.add(value);
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public PageHeader writePage(
      final Encoding encoding, final ColumnMetaData columnMetaData, final OutputStream outputStream)
      throws IOException {
    final var pageOutputBufferStream = new ByteBufferOutputStream();

    final var repetitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        repetitionLevels,
        IntEncodings.bitWidth(
            columnChunkWriter.getColumnType().schemaNode().getRepetitionLevelMax()),
        repetitionLevelsOutputStream);
    repetitionLevelsOutputStream.close();
    final var definitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        definitionLevels,
        IntEncodings.bitWidth(
            columnChunkWriter.getColumnType().schemaNode().getDefinitionLevelMax()),
        definitionLevelsOutputStream);
    definitionLevelsOutputStream.close();

    final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedValuesOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));
    Encodings.<Value>getEncoding(encoding)
        .encode(values, uncompressedValuesOutputStream, columnChunkWriter);
    uncompressedValuesOutputStream.close();

    final var levelsBytesWritten =
        repetitionLevelsOutputStream.getBytesWrittenAsInt()
            + definitionLevelsOutputStream.getBytesWrittenAsInt();

    final var pageHeader =
        new PageHeader(
            PageType.DATA_PAGE_V2,
            levelsBytesWritten + uncompressedValuesOutputStream.getBytesWrittenAsInt(),
            levelsBytesWritten + compressedValuesOutputStream.getBytesWrittenAsInt());
    // TODO - we could look at counting rows... seems expensive with current structure though
    // TODO - separate pages from column chunks - allow multiple smaller pages
    pageHeader.data_page_header_v2 =
        dataPageHeaderV2
            .setIs_compressed(!columnMetaData.codec.equals(CompressionCodec.UNCOMPRESSED))
            .setEncoding(encoding)
            .setRepetition_levels_byte_length(repetitionLevelsOutputStream.getBytesWrittenAsInt())
            .setDefinition_levels_byte_length(definitionLevelsOutputStream.getBytesWrittenAsInt());

    Util.writePageHeader(pageHeader, outputStream);
    pageOutputBufferStream.writeTo(outputStream);

    return pageHeader;
  }

  @Override
  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_values;
  }

  @Override
  public long getNumValues() {
    return dataPageHeaderV2.num_values;
  }

  @Override
  public long getNumNulls(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_nulls;
  }

  @Override
  public long getNumNulls() {
    return dataPageHeaderV2.num_nulls;
  }
}
