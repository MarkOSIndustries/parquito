package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DeltaLengthByteArrayEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var lengths = DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageBuffer);
    final var bytes = decompressedPageBuffer.slice();
    final var buffers = new ByteBuffer[lengths.length];
    int offset = 0;
    for (int i = 0; i < lengths.length; i++) {
      buffers[i] = bytes.slice(offset, lengths[i]);
      offset += lengths[i];
    }

    return new Values.Impl() {
      @Override
      public ByteBuffer getByteBuffer(final int index) {
        return buffers[index];
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  @Override
  public void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException {
    final var lengths = new int[values.length()];
    var totalBytesForValues = 0;
    for (var i = 0; i < values.length(); i++) {
      totalBytesForValues += (lengths[i] = values.getAsByteBuffer(i).remaining());
    }

    DeltaBinaryPackedEncoding.encode32(lengths, uncompressedPageStream);

    final var valueBuffer = ByteBuffer.allocate(totalBytesForValues);
    for (var i = 0; i < values.length(); i++) {
      valueBuffer.put(values.getAsByteBuffer(i));
    }
    uncompressedPageStream.write(valueBuffer.array());
  }

  @Override
  public int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return estimatedPlainBytesRequired + 4 * values.length();
  }
}
