package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntConsumer;

public class ByteStreamSplitEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var type = columnChunkReader.getColumnType().getType();
    final var byteWidth = FixedTypeLengths.BYTES_BY_TYPE.get(type);

    final int decompressedPageBytes = decompressedPageBuffer.remaining();
    if (decompressedPageBytes % byteWidth != 0) {
      throw new IllegalArgumentException(
          "Number of bytes should be divisible by byteWidth - but "
              + decompressedPageBytes
              + " mod "
              + byteWidth
              + " ≠ 0");
    }

    if (decompressedPageBytes != expectedValues * byteWidth) {
      throw new IllegalArgumentException(
          "There should be "
              + expectedValues * byteWidth
              + " bytes, but we only have "
              + decompressedPageBytes);
    }

    abstract class BufferExtractor extends Values.Impl {
      private final ByteBuffer buffer =
          ByteBuffer.allocate(byteWidth).order(ByteOrder.LITTLE_ENDIAN);

      protected ByteBuffer bufferValueAtIndex(int index) {
        var byteIndex = 0;
        for (var streamIndex = index;
            streamIndex < decompressedPageBuffer.remaining();
            streamIndex += expectedValues) {
          buffer.put(byteIndex++, decompressedPageBuffer.get(streamIndex));
        }
        return buffer;
      }

      @Override
      public int count() {
        return expectedValues;
      }
    }

    return switch (type) {
      case INT32 ->
          new BufferExtractor() {
            @Override
            public int getInt32(final int index) {
              return bufferValueAtIndex(index).getInt(0);
            }
          };
      case INT64 ->
          new BufferExtractor() {
            @Override
            public long getInt64(final int index) {
              return bufferValueAtIndex(index).getLong(0);
            }
          };
      case FLOAT ->
          new BufferExtractor() {
            @Override
            public float getFloat(final int index) {
              return bufferValueAtIndex(index).getFloat(0);
            }
          };
      case DOUBLE ->
          new BufferExtractor() {
            @Override
            public double getDouble(final int index) {
              return bufferValueAtIndex(index).getDouble(0);
            }
          };
      default ->
          throw new UnsupportedOperationException(
              this.getClass().getSimpleName() + " does not support type " + type);
    };
  }

  @Override
  public void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException {
    final var byteWidth = FixedTypeLengths.BYTES_BY_TYPE.get(values.getType());
    final var splitStreams = new byte[byteWidth][];
    for (var i = 0; i < splitStreams.length; i++) {
      splitStreams[i] = new byte[values.length()];
    }

    final var buffer = ByteBuffer.allocate(byteWidth).order(ByteOrder.LITTLE_ENDIAN);
    final IntConsumer writeValueAtIndexToBuffer =
        switch (values.getType()) {
          case INT32 -> index -> buffer.putInt(values.getAsInt32(index));
          case INT64 -> index -> buffer.putLong(values.getAsInt64(index));
          case FLOAT -> index -> buffer.putFloat(values.getAsFloat(index));
          case DOUBLE -> index -> buffer.putDouble(values.getAsDouble(index));
          default ->
              throw new UnsupportedOperationException(
                  this.getClass().getSimpleName() + " does not support type " + values.getType());
        };
    for (var valueIndex = 0; valueIndex < values.length(); valueIndex++) {
      writeValueAtIndexToBuffer.accept(valueIndex);
      for (var byteIndex = 0; byteIndex < byteWidth; byteIndex++) {
        splitStreams[byteIndex][valueIndex] = buffer.array()[byteIndex];
      }
      buffer.rewind();
    }

    for (final var splitStream : splitStreams) {
      uncompressedPageStream.write(splitStream);
    }
  }

  @Override
  public int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return estimatedPlainBytesRequired;
  }
}
