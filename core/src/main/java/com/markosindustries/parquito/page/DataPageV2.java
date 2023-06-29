package com.markosindustries.parquito.page;

import static com.markosindustries.parquito.encoding.IntEncodings.INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER;

import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public class DataPageV2<ReadAs> implements DataPage<ReadAs> {
  private final int[] repetitionLevels;
  private final int[] definitionLevels;
  private final PageHeader pageHeader;
  private final int totalValues;
  private final int nonNullValues;
  private final Values<ReadAs> values;

  protected DataPageV2(
      final PageHeader pageHeader,
      final ColumnChunkReader<ReadAs> columnChunkReader,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var pageStream = new ByteBufferInputStream(pageBuffer);

    this.repetitionLevels =
        INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.decode(
            pageHeader.data_page_header_v2.num_values,
            IntEncodings.bitWidth(
                columnChunkReader.getColumnType().schemaNode().getRepetitionLevelMax()),
            pageStream);
    this.definitionLevels =
        INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.decode(
            pageHeader.data_page_header_v2.num_values,
            IntEncodings.bitWidth(
                columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax()),
            pageStream);

    this.totalValues = pageHeader.data_page_header_v2.num_values;
    this.nonNullValues =
        pageHeader.data_page_header_v2.num_values - pageHeader.data_page_header_v2.num_nulls;

    if (nonNullValues == 0) {
      this.values = Values.empty();
    } else {
      final var decompressedPageStream =
          (pageHeader.data_page_header_v2.isSetIs_compressed()
                  && !pageHeader.data_page_header_v2.is_compressed)
              ? pageStream
              : CompressionCodecs.decompress(
                  columnChunkReader.getHeader().meta_data.codec, pageStream);
      this.values =
          Encodings.<ReadAs>getDecoder(pageHeader.data_page_header_v2.encoding)
              .decode(
                  nonNullValues,
                  pageHeader.uncompressed_page_size,
                  decompressedPageStream,
                  columnChunkReader);
    }
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
