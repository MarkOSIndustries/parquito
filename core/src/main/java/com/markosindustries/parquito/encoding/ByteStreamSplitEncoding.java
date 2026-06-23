package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

public class ByteStreamSplitEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var type = columnChunkReader.getColumnType().getType();
    final var byteWidth = FixedTypeLengths.BYTES_BY_TYPE.get(type);

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

    final var bytes = decompressedPageStream.readAllBytes();
    if (bytes.length != decompressedPageBytes) {
      throw new IllegalArgumentException(
          "There should be " + decompressedPageBytes + " bytes, but we only have " + bytes.length);
    }

    final var buffer = ByteBuffer.allocate(byteWidth).order(ByteOrder.LITTLE_ENDIAN);
    final BiConsumer<Integer, Values.Visitor> visitBuffer =
        switch (type) {
          case INT32 -> (pageIndex, visitor) -> visitor.visit(pageIndex, buffer.getInt(0));
          case INT64 -> (pageIndex, visitor) -> visitor.visit(pageIndex, buffer.getLong(0));
          case FLOAT -> (pageIndex, visitor) -> visitor.visit(pageIndex, buffer.getFloat(0));
          case DOUBLE -> (pageIndex, visitor) -> visitor.visit(pageIndex, buffer.getDouble(0));
          default ->
              throw new UnsupportedOperationException(
                  this.getClass().getSimpleName() + " does not support type " + type);
        };
    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        var byteIndex = 0;
        for (var streamIndex = valueIndex;
            streamIndex < bytes.length;
            streamIndex += expectedValues) {
          buffer.put(byteIndex++, bytes[streamIndex]);
        }
        visitBuffer.accept(pageIndex, visitor);
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
