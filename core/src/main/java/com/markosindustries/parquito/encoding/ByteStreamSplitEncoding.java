package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.types.ParquetType;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteStreamSplitEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    final var parquetType = columnChunkReader.getColumnType().parquetType();
    final var byteWidth = getByteWidth(parquetType);

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

    var buffer = ByteBuffer.allocate(byteWidth);
    return index -> {
      var byteIndex = 0;
      for (var streamIndex = index; streamIndex < bytes.length; streamIndex += expectedValues) {
        buffer.put(byteIndex++, bytes[streamIndex]);
      }
      return parquetType.readFromByteBuffer(buffer);
    };
  }

  @Override
  public void encode(
      final FastDictionary<ReadAs, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    final var parquetType = columnChunkWriter.getColumnType().parquetType();
    final var byteWidth = getByteWidth(parquetType);
    final var splitStreams = new byte[byteWidth][];
    for (var i = 0; i < splitStreams.length; i++) {
      splitStreams[i] = new byte[values.length()];
    }

    final var buffer = ByteBuffer.allocate(byteWidth);
    for (var valueIndex = 0; valueIndex < values.length(); valueIndex++) {
      parquetType.writeToByteBuffer(values.getAsObject(valueIndex), buffer);
      for (var byteIndex = 0; byteIndex < byteWidth; byteIndex++) {
        splitStreams[byteIndex][valueIndex] = buffer.array()[byteIndex];
      }
    }

    for (final var splitStream : splitStreams) {
      uncompressedPageStream.write(splitStream);
    }
  }

  @Override
  public int refineBytesRequiredEstimate(
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<ReadAs> columnChunkWriter) {
    return estimatedPlainBytesRequired;
  }

  private static int getByteWidth(final ParquetType<?> parquetType) {
    // we can pass null here because ByteStreamSplit only applies to fixed-size data
    // none of the possible implementations will look at the value
    return parquetType.getRequiredBytesToWrite(null);
  }
}
