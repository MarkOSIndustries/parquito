package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunk;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;

public interface ParquetEncoding<ReadAs extends Comparable<ReadAs>> {
  Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      ColumnChunk<ReadAs> columnChunk)
      throws IOException;
}
