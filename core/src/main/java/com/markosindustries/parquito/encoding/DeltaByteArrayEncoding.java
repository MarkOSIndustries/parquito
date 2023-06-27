package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DeltaByteArrayEncoding<ReadAs extends Comparable<ReadAs>>
    implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    final var prefixLengths =
        IntEncodings.INT_ENCODING_DELTA_BINARY_PACKED.decode(
            expectedValues, -1, decompressedPageStream);
    final var lengths =
        IntEncodings.INT_ENCODING_DELTA_BINARY_PACKED.decode(
            expectedValues, -1, decompressedPageStream);
    final var offsets = new int[lengths.length];
    {
      int offset = 0;
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] += offset;
        offset += lengths[i];
      }
    }
    final var bytes = ByteBuffer.wrap(decompressedPageStream.readAllBytes());

    return index -> {
      if (prefixLengths[index] == 0) {
        return columnChunkReader.read(bytes.slice(offsets[index], lengths[index]));
      }
      if (lengths[index] == 0) {
        return columnChunkReader.read(bytes.slice(offsets[index - 1], prefixLengths[index]));
      }
      final var prefix = bytes.slice(offsets[index - 1], prefixLengths[index]);
      final var suffix = bytes.slice(offsets[index], lengths[index]);
      final var concat = ByteBuffer.allocate(prefix.capacity() + suffix.capacity());
      concat.put(prefix);
      concat.put(suffix);
      return columnChunkReader.read(concat);
    };
  }
}
