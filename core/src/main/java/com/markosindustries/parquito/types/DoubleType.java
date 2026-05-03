package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import org.apache.parquet.format.LogicalType;

public abstract class DoubleType<ReadAs> extends ParquetType<ReadAs> {
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
    if (inputStream.readNBytes(buffer.array(), 0, expectedBytes) != expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Doubles");
    }

    final var doubleBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();

    return index -> wrap(doubleBuffer.get(index));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(0));
  }

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var requiredBytes = values.size() * 8;
    final var buffer = ByteBuffer.allocate(requiredBytes);
    final var doubleBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
    doubleBuffer.position(0);
    for (final var value : values) {
      doubleBuffer.put(unwrap(value));
    }
    outputStream.write(buffer.array());
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return 8;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().put(unwrap(value));
  }

  protected abstract ReadAs wrap(final double value);

  protected abstract double unwrap(final ReadAs value);

  private static final DoubleType<Double> DOUBLES =
      new DoubleType<Double>(Double.class) {
        @Override
        protected Double wrap(final double value) {
          return value;
        }

        @Override
        protected double unwrap(final Double value) {
          return value;
        }

        @Override
        public int compare(final Double o1, final Double o2) {
          return o1.compareTo(o2);
        }
      };

  public static DoubleType<?> create(final LogicalType logicalType) {
    return DOUBLES;
  }
}
