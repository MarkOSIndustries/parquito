package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;

public class PlainEncoding implements ParquetEncoding {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).mark();

  @Override
  public Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    return switch (columnChunkReader.getColumnType().getType()) {
      case BOOLEAN -> decodeBooleans(expectedValues, decompressedPageBuffer);
      case INT32 -> decodeInt32s(expectedValues, decompressedPageBuffer);
      case INT64 -> decodeInt64s(expectedValues, decompressedPageBuffer);
      case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
      case FLOAT -> decodeFloats(expectedValues, decompressedPageBuffer);
      case DOUBLE -> decodeDoubles(expectedValues, decompressedPageBuffer);
      case BYTE_ARRAY -> decodeVariableBytes(expectedValues, decompressedPageBuffer);
      case FIXED_LEN_BYTE_ARRAY ->
          decodeFixedBytes(
              columnChunkReader.getColumnType().schemaNode().getTypeLength(),
              expectedValues,
              decompressedPageBuffer);
    };
  }

  private Values decodeBooleans(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final int[] values =
        IntEncodings.INT_ENCODING_BIT_PACKED.decode(expectedValues, 1, decompressedPageBuffer);
    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, values[valueIndex] != 0);
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeInt32s(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var expectedBytes = expectedValues * 4;
    if (decompressedPageBuffer.remaining() < expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Int32s");
    }
    final var intBuffer =
        decompressedPageBuffer.slice(0, expectedBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, intBuffer.get(valueIndex));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeInt64s(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var expectedBytes = expectedValues * 8;
    if (decompressedPageBuffer.remaining() < expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Int64s");
    }
    final var longBuffer =
        decompressedPageBuffer
            .slice(0, expectedBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer();

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, longBuffer.get(valueIndex));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeFloats(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var expectedBytes = expectedValues * 4;
    if (decompressedPageBuffer.remaining() < expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Floats");
    }
    final var floatBuffer =
        decompressedPageBuffer
            .slice(0, expectedBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asFloatBuffer();

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, floatBuffer.get(valueIndex));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeDoubles(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var expectedBytes = expectedValues * 8;
    if (decompressedPageBuffer.remaining() < expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Doubles");
    }
    final var doubleBuffer =
        decompressedPageBuffer
            .slice(0, expectedBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asDoubleBuffer();

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, doubleBuffer.get(valueIndex));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeVariableBytes(
      final int expectedValues, final ByteBuffer decompressedPageBuffer) throws IOException {
    decompressedPageBuffer.order(ByteOrder.LITTLE_ENDIAN);
    final var buffers = new ByteBuffer[expectedValues];
    for (int bufferIndex = decompressedPageBuffer.position(), i = 0; i < expectedValues; i++) {
      final var size = decompressedPageBuffer.getInt(bufferIndex);
      bufferIndex += 4;
      if (size > 0) {
        buffers[i] = decompressedPageBuffer.slice(bufferIndex, size).mark();
        bufferIndex += size;
      } else {
        buffers[i] = EMPTY_BUFFER;
      }
    }

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, buffers[valueIndex].reset());
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  private Values decodeFixedBytes(
      final int typeLength, final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var totalBytes = expectedValues * typeLength;
    final var bytes = decompressedPageBuffer.slice(decompressedPageBuffer.position(), totalBytes);
    final var buffers = new ByteBuffer[expectedValues];
    var offset = 0;
    for (var i = 0; i < expectedValues; i++) {
      buffers[i] = bytes.slice(offset, typeLength).mark();
      offset += typeLength;
    }

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, buffers[valueIndex].reset());
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
    switch (values.getType()) {
      case BOOLEAN -> {
        IntEncodings.INT_ENCODING_BIT_PACKED.encode(
            values.getBooleansAsIntList(), 1, uncompressedPageStream);
      }
      case INT32 -> {
        final var requiredBytes = values.length() * 4;
        final var buffer = ByteBuffer.allocate(requiredBytes);
        final var intBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        intBuffer.position(0);
        for (var i = 0; i < values.length(); i++) {
          intBuffer.put(values.getAsInt32(i));
        }
        uncompressedPageStream.write(buffer.array());
      }
      case INT64 -> {
        final var requiredBytes = values.length() * 8;
        final var buffer = ByteBuffer.allocate(requiredBytes);
        final var longBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
        longBuffer.position(0);
        for (var i = 0; i < values.length(); i++) {
          longBuffer.put(values.getAsInt64(i));
        }
        uncompressedPageStream.write(buffer.array());
      }
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> {
        final var requiredBytes = values.length() * 4;
        final var buffer = ByteBuffer.allocate(requiredBytes);
        final var floatBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
        floatBuffer.position(0);
        for (var i = 0; i < values.length(); i++) {
          floatBuffer.put(values.getAsFloat(i));
        }
        uncompressedPageStream.write(buffer.array());
      }
      case DOUBLE -> {
        final var requiredBytes = values.length() * 8;
        final var buffer = ByteBuffer.allocate(requiredBytes);
        final var doubleBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
        doubleBuffer.position(0);
        for (var i = 0; i < values.length(); i++) {
          doubleBuffer.put(values.getAsDouble(i));
        }
        uncompressedPageStream.write(buffer.array());
      }
      case BYTE_ARRAY -> {
        final var writableChannel = Channels.newChannel(uncompressedPageStream);
        for (var i = 0; i < values.length(); i++) {
          final var value = values.getAsByteBuffer(i);
          LittleEndian.writeInt(value.remaining(), uncompressedPageStream);
          writableChannel.write(value);
        }
      }
      case FIXED_LEN_BYTE_ARRAY -> {
        final var writableChannel = Channels.newChannel(uncompressedPageStream);
        for (var i = 0; i < values.length(); i++) {
          writableChannel.write(values.getAsByteBuffer(i));
        }
      }
    }
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
