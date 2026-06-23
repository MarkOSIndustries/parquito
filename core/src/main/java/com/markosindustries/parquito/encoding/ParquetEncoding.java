package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface ParquetEncoding {
  Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException;

  void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException;

  int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired);
}
