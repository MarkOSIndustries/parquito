package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import org.apache.parquet.format.LogicalType;

public abstract class Int32Type<ReadAs extends Comparable<ReadAs>> extends ParquetType<ReadAs> {
  protected Int32Type(final Class<ReadAs> readAsClass) {
    super(readAsClass);
  }

  @Override
  public Values<ReadAs> readPlainPage(
      final int expectedValues, final int decompressedPageBytes, final InputStream inputStream)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var expectedBytes = expectedValues * 4;
    final var buffer = ByteBuffer.allocate(expectedBytes);
    if (inputStream.read(buffer.array()) != expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Int32s");
    }

    final var intBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    return index -> wrap(intBuffer.get(index));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(0));
  }

  protected abstract ReadAs wrap(final int value);

  private static final Int32Type<Integer> INTEGERS =
      new Int32Type<Integer>(Integer.class) {
        @Override
        protected Integer wrap(final int value) {
          return value;
        }
      };
  private static final Int32Type<Instant> DATES =
      new Int32Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final int value) {
          return Instant.ofEpochSecond(Duration.ofDays(value).getSeconds());
        }
      };

  public static Int32Type<?> create(final LogicalType logicalType) {
    if (logicalType != null) {
      if (logicalType.isSetDATE()) {
        return DATES;
      }
    }

    return INTEGERS;
  }
}
