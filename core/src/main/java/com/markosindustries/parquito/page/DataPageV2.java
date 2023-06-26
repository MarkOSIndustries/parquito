package com.markosindustries.parquito.page;

import static com.markosindustries.parquito.encoding.IntEncodings.INT_ENCODING_RLE;

import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ColumnChunk;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public class DataPageV2<ReadAs extends Comparable<ReadAs>> implements DataPage<ReadAs> {
  private final int[] repetitionLevels;
  private final int[] definitionLevels;
  private final PageHeader pageHeader;
  private final int totalValues;
  private final int nonNullValues;
  private final Values<ReadAs> values;

  protected DataPageV2(
      final PageHeader pageHeader,
      final ColumnChunk<ReadAs> columnChunk,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var pageStream = new ByteBufferInputStream(pageBuffer);

    this.repetitionLevels =
        INT_ENCODING_RLE.decode(
            pageHeader.data_page_header_v2.num_values,
            IntEncodings.bitWidth(columnChunk.getColumnType().schemaNode().getRepetitionLevelMax()),
            pageStream);
    this.definitionLevels =
        INT_ENCODING_RLE.decode(
            pageHeader.data_page_header_v2.num_values,
            IntEncodings.bitWidth(columnChunk.getColumnType().schemaNode().getDefinitionLevelMax()),
            pageStream);
    final var decompressedPageStream =
        (pageHeader.data_page_header_v2.isSetIs_compressed()
                && !pageHeader.data_page_header_v2.is_compressed)
            ? pageStream
            : CompressionCodecs.decompress(columnChunk.getHeader().meta_data.codec, pageStream);

    this.totalValues = pageHeader.data_page_header_v2.num_values;
    this.nonNullValues =
        pageHeader.data_page_header_v2.num_values - pageHeader.data_page_header_v2.num_nulls;
    this.values =
        Encodings.<ReadAs>getDecoder(pageHeader.data_page_header.encoding)
            .decode(
                nonNullValues,
                pageHeader.uncompressed_page_size,
                decompressedPageStream,
                columnChunk);
  }

  @Override
  public int[] getRepetitionLevels() {
    return repetitionLevels;
  }

  @Override
  public int[] getDefinitionLevels() {
    return definitionLevels;
  }

  @Override
  public PageHeader getPageHeader() {
    return pageHeader;
  }

  @Override
  public int getTotalValues() {
    return totalValues;
  }

  @Override
  public int getNonNullValues() {
    return nonNullValues;
  }

  @Override
  public ReadAs getValue(final int index) {
    return values.get(index);
  }
}
