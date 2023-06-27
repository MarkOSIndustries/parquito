package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;

public class DictionaryPage<ReadAs extends Comparable<ReadAs>> implements ParquetPage<ReadAs> {
  private final PageHeader pageHeader;
  private final Values<ReadAs> values;

  public DictionaryPage(
      final PageHeader pageHeader,
      final ColumnChunkReader<ReadAs> columnChunkReader,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var decompressedPageStream =
        CompressionCodecs.decompress(
            columnChunkReader.getHeader().meta_data.codec, new ByteBufferInputStream(pageBuffer));

    this.values =
        Encodings.<ReadAs>getDecoder(Encoding.PLAIN)
            .decode(
                pageHeader.dictionary_page_header.num_values,
                pageHeader.uncompressed_page_size,
                decompressedPageStream,
                columnChunkReader);
  }

  @Override
  public PageHeader getPageHeader() {
    return pageHeader;
  }

  @Override
  public int getTotalValues() {
    return pageHeader.dictionary_page_header.num_values;
  }

  @Override
  public int getNonNullValues() {
    return pageHeader.dictionary_page_header.num_values;
  }

  @Override
  public ReadAs getValue(final int index) {
    return values.get(index);
  }
}
