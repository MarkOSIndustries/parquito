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
    this.dataPageHeaderV2 = new DataPageHeaderV2().setNum_values(0).setNum_nulls(0);
    this.values = new ArrayList<>();
    this.definitionLevels = new IntArrayList();
    this.repetitionLevels = new IntArrayList();
  }

  @Override
  public void addNull(final int repetitionLevel, final int definitionLevel) {
    dataPageHeaderV2.num_nulls++;
    dataPageHeaderV2.num_values++;
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public void addValue(final Value value, final int repetitionLevel, final int definitionLevel) {
    dataPageHeaderV2.num_values++;
    values.add(value);
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public PageHeader writePage(final ColumnMetaData columnMetaData, final OutputStream outputStream)
      throws IOException {
    final var pageOutputBufferStream = new ByteBufferOutputStream();

    // TODO - drive this off the schema node type and maybe something about value cardinality,
    // ordering, etc
    final Encoding selectedEncoding = Encoding.PLAIN;

    final var repetitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        repetitionLevels,
        IntEncodings.bitWidth(
            columnChunkWriter.getColumnType().schemaNode().getRepetitionLevelMax()),
        repetitionLevelsOutputStream);
    repetitionLevelsOutputStream.flush();
    final var definitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        definitionLevels,
        IntEncodings.bitWidth(
            columnChunkWriter.getColumnType().schemaNode().getDefinitionLevelMax()),
        definitionLevelsOutputStream);
    definitionLevelsOutputStream.flush();

    final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedValuesOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));
    Encodings.<Value>getEncoding(columnMetaData.encodings.getFirst())
        .encode(values, uncompressedValuesOutputStream, columnChunkWriter);
    uncompressedValuesOutputStream.flush();

    final var levelsBytesWritten =
        repetitionLevelsOutputStream.getBytesWritten()
            + definitionLevelsOutputStream.getBytesWritten();

    final var pageHeader =
        new PageHeader(
            PageType.DATA_PAGE_V2,
            levelsBytesWritten + uncompressedValuesOutputStream.getBytesWritten(),
            levelsBytesWritten + compressedValuesOutputStream.getBytesWritten());
    // TODO - we could look at counting rows... seems expensive with current structure though
    // TODO - separate pages from column chunks - allow multiple smaller pages
    pageHeader.data_page_header_v2 =
        dataPageHeaderV2
            .setIs_compressed(!columnMetaData.codec.equals(CompressionCodec.UNCOMPRESSED))
            .setEncoding(selectedEncoding)
            .setRepetition_levels_byte_length(repetitionLevelsOutputStream.getBytesWritten())
            .setDefinition_levels_byte_length(definitionLevelsOutputStream.getBytesWritten());

    Util.writePageHeader(pageHeader, outputStream);
    pageOutputBufferStream.writeTo(outputStream);

    return pageHeader;
  }

  @Override
  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_values;
  }

  @Override
  public long getNumNulls(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_nulls;
  }
}
