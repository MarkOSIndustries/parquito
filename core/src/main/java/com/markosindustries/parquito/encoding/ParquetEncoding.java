package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface ParquetEncoding<ReadAs> {
  Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException;

  void encode(
      final List<ReadAs> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException;
}
