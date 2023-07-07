package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DeltaByteArrayEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    final var prefixLengths =
        DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
    final var lengths = DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
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
        return columnChunkReader.readValue(bytes.slice(offsets[index], lengths[index]));
      }
      if (lengths[index] == 0) {
        return columnChunkReader.readValue(bytes.slice(offsets[index - 1], prefixLengths[index]));
      }
      final var concat = ByteBuffer.allocate(prefixLengths[index] + lengths[index]);
      concat.put(prefixLengths[index], bytes, offsets[index], lengths[index]);
      int prevIndex = index, bytesNeeded = prefixLengths[index];
      do {
        prevIndex--;
        if (bytesNeeded > prefixLengths[prevIndex]) {
          final var bytesAvailable = bytesNeeded - prefixLengths[prevIndex];
          concat.put(prefixLengths[prevIndex], bytes, offsets[prevIndex], bytesAvailable);
          bytesNeeded -= bytesAvailable;
        }
      } while (prefixLengths[prevIndex] != 0);
      return columnChunkReader.readValue(concat);
    };
  }
}
