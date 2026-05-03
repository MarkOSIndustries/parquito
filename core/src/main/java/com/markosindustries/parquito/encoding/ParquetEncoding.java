package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ParquetEncoding<ReadAs> {
  Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException;

  void encode(
      final FastDictionary<ReadAs, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException;

  int refineBytesRequiredEstimate(
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<ReadAs> columnChunkWriter);
}
