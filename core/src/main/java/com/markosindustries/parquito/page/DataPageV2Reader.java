package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ByteCountingInputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.encoding.Maths;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public class DataPageV2Reader implements DataPageReader {
  private final int[] repetitionLevels;
  private final int[] definitionLevels;
  private final PageHeader pageHeader;
  private final int totalValues;
  private final int nonNullValues;
  private final Values values;

  protected DataPageV2Reader(
      final PageHeader pageHeader,
      final ColumnChunkReader columnChunkReader,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var pageStream = new ByteCountingInputStream(new ByteBufferInputStream(pageBuffer));

    this.repetitionLevels =
        IntEncodings.INT_ENCODING_DATA_PAGE_V2_LEVELS.decode(
            pageHeader.data_page_header_v2.num_values,
            Maths.bitWidth(columnChunkReader.getColumnType().schemaNode().getRepetitionLevelMax()),
            pageStream);
    this.definitionLevels =
        IntEncodings.INT_ENCODING_DATA_PAGE_V2_LEVELS.decode(
            pageHeader.data_page_header_v2.num_values,
            Maths.bitWidth(columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax()),
            pageStream);

    final var bytesInLevels = pageStream.getBytesReadAsInt();

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
          Encodings.getEncoding(pageHeader.data_page_header_v2.encoding)
              .decode(
                  nonNullValues,
                  pageHeader.uncompressed_page_size - bytesInLevels,
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
  public Values getValues() {
    return values;
  }
}
