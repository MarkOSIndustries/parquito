package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.format.LogicalType;

public abstract class DoubleType<ReadAs extends Comparable<ReadAs>> extends ParquetType<ReadAs> {
  public DoubleType(final Class<ReadAs> doubleClass) {
    super(doubleClass);
  }

  @Override
  public Values<ReadAs> readPlainPage(
      final int expectedValues, final int decompressedPageBytes, final InputStream inputStream)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var expectedBytes = expectedValues * 8;
    final var buffer = ByteBuffer.allocate(expectedBytes);
    if (inputStream.read(buffer.array()) != expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Doubles");
    }

    final var doubleBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
    return index -> wrap(doubleBuffer.get(index));
  }

  @Override
  public ReadAs readColumnStatsValue(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(0));
  }

  protected abstract ReadAs wrap(final double value);

  private static final DoubleType<Double> DOUBLES =
      new DoubleType<Double>(Double.class) {
        @Override
        protected Double wrap(final double value) {
          return value;
        }
      };

  public static DoubleType<?> create(final LogicalType logicalType) {
    return DOUBLES;
  }
}
