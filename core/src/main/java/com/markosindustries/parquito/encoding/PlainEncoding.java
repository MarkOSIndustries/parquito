package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunk;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;

public class PlainEncoding<ReadAs extends Comparable<ReadAs>> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunk<ReadAs> columnChunk)
      throws IOException {
    return columnChunk
        .getColumnType()
        .parquetType()
        .readPlainPage(expectedValues, decompressedPageBytes, decompressedPageStream);
  }
}
