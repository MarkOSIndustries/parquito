package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public class DeltaByteArrayEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).limit(0);

  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
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

    return index -> {
      if (prefixLengths[index] == 0) {
        return columnChunkReader.readValue(bytes.slice(offsets[index], suffixLengths[index]));
      }
      final var concat = ByteBuffer.allocate(prefixLengths[index] + suffixLengths[index]);
      concat.put(prefixLengths[index], bytes, offsets[index], suffixLengths[index]);
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

  @Override
  public void encode(
      final FastDictionary<ReadAs, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    var previousBuffer = EMPTY_BUFFER;
    var currentBuffer = EMPTY_BUFFER;
    final var valuesLength = values.length();
    final var prefixCounts = new int[valuesLength];
    final var suffixCounts = new int[valuesLength];
    final var valuesOutputBufferStream = new ByteBufferOutputStream(valuesLength);
    final var valuesOutputBufferChannel = Channels.newChannel(valuesOutputBufferStream);
    for (var valueIndex = 0; valueIndex < valuesLength; valueIndex++) {
      final var value = values.getAsObject(valueIndex);
      final var requiredCapacity =
          columnChunkWriter.getColumnType().parquetType().getRequiredBytesToWrite(value);
      if (requiredCapacity > currentBuffer.capacity()) {
        previousBuffer = resizeBuffer(previousBuffer, requiredCapacity);
        currentBuffer = resizeBuffer(currentBuffer, requiredCapacity);
      }

      currentBuffer.limit(requiredCapacity);
      currentBuffer.position(0);
      columnChunkWriter.getColumnType().parquetType().writeToByteBuffer(value, currentBuffer);
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
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<ReadAs> columnChunkWriter) {
    return estimatedPlainBytesRequired
        + columnChunkWriter.getColumnType().parquetType().getPlainBytesOverhead() * valueCount;
  }
}
