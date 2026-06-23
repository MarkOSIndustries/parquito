package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public class DeltaByteArrayEncoding implements ParquetEncoding {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).limit(0);

  @Override
  public Values decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var prefixLengths =
        DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
    final var suffixLengths =
        DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
    final var offsets = new int[suffixLengths.length];
    {
      int offset = 0;
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] += offset;
        offset += suffixLengths[i];
      }
    }
    final var bytes = ByteBuffer.wrap(decompressedPageStream.readAllBytes());

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        if (prefixLengths[valueIndex] == 0) {
          visitor.visit(pageIndex, bytes.slice(offsets[valueIndex], suffixLengths[valueIndex]));
          return;
        }

        final var concat =
            ByteBuffer.allocate(prefixLengths[valueIndex] + suffixLengths[valueIndex]);
        concat.put(
            prefixLengths[valueIndex], bytes, offsets[valueIndex], suffixLengths[valueIndex]);
        int prevIndex = valueIndex, bytesNeeded = prefixLengths[valueIndex];
        do {
          prevIndex--;
          if (bytesNeeded > prefixLengths[prevIndex]) {
            final var bytesAvailable = bytesNeeded - prefixLengths[prevIndex];
            concat.put(prefixLengths[prevIndex], bytes, offsets[prevIndex], bytesAvailable);
            bytesNeeded -= bytesAvailable;
          }
        } while (prefixLengths[prevIndex] != 0);
        visitor.visit(pageIndex, concat);
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
    var previousBuffer = EMPTY_BUFFER;
    var currentBuffer = EMPTY_BUFFER;
    final var valuesLength = values.length();
    final var prefixCounts = new int[valuesLength];
    final var suffixCounts = new int[valuesLength];
    final var valuesOutputBufferStream = new ByteBufferOutputStream(valuesLength);
    final var valuesOutputBufferChannel = Channels.newChannel(valuesOutputBufferStream);
    for (var valueIndex = 0; valueIndex < valuesLength; valueIndex++) {
      final var value = values.getAsByteBuffer(valueIndex);
      final var requiredCapacity = value.remaining();
      if (requiredCapacity > currentBuffer.capacity()) {
        previousBuffer = resizeBuffer(previousBuffer, requiredCapacity);
        currentBuffer = resizeBuffer(currentBuffer, requiredCapacity);
      }

      currentBuffer.limit(requiredCapacity);
      currentBuffer.position(0);
      currentBuffer.put(value);
      var prefixCount = 0;
      for (var i = 0; i < previousBuffer.limit() && i < currentBuffer.limit(); i++) {
        if (previousBuffer.get(i) != currentBuffer.get(i)) {
          break;
        }
        prefixCount++;
      }
      prefixCounts[valueIndex] = prefixCount;
      suffixCounts[valueIndex] = requiredCapacity - prefixCount;
      valuesOutputBufferChannel.write(
          currentBuffer.slice(prefixCounts[valueIndex], suffixCounts[valueIndex]));

      final var tmp = currentBuffer;
      currentBuffer = previousBuffer;
      previousBuffer = tmp;
    }

    DeltaBinaryPackedEncoding.encode32(prefixCounts, uncompressedPageStream);
    DeltaBinaryPackedEncoding.encode32(suffixCounts, uncompressedPageStream);
    valuesOutputBufferStream.writeTo(uncompressedPageStream);
  }

  private ByteBuffer resizeBuffer(final ByteBuffer buffer, final int requiredCapacity) {
    final var newBuffer = ByteBuffer.allocate(requiredCapacity);
    System.arraycopy(buffer.array(), 0, newBuffer.array(), 0, buffer.capacity());
    newBuffer.limit(buffer.limit());
    newBuffer.position(buffer.position());
    return newBuffer;
  }

  @Override
  public int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return switch (values.getType()) {
      case BYTE_ARRAY -> estimatedPlainBytesRequired + 4 * values.length();
      default -> estimatedPlainBytesRequired;
    };
  }
}
