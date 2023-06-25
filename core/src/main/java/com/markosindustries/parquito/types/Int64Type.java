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

public abstract class Int64Type<ReadAs extends Comparable<ReadAs>> extends ParquetType<ReadAs> {
  public Int64Type(final Class<ReadAs> readAsClass) {
    super(readAsClass);
  }

  @Override
  public Values<ReadAs> readPlainPage(
      int expectedValues, final int decompressedPageBytes, InputStream inputStream)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var expectedBytes = expectedValues * 8;
    final var buffer = ByteBuffer.allocate(expectedBytes);
    if (inputStream.read(buffer.array()) != expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Int64s");
    }

    final var longBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    return index -> wrap(longBuffer.get(index));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(0));
  }

  protected abstract ReadAs wrap(final long value);

  private static final Int64Type<Long> LONGS =
      new Int64Type<Long>(Long.class) {
        @Override
        protected Long wrap(final long value) {
          return value;
        }
      };
  private static final Int64Type<Duration> TIME_MILLIS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofMillis(value);
        }
      };
  private static final Int64Type<Duration> TIME_MICROS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofNanos(value * 1_000);
        }
      };
  private static final Int64Type<Duration> TIME_NANOS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofNanos(value);
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_MILLIS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          return Instant.ofEpochMilli(value);
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_MICROS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          final var epochSeconds = Math.floorDiv(value, 1_000_000);
          return Instant.ofEpochSecond(epochSeconds, value - (epochSeconds * 1_000_000));
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_NANOS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          final var epochSeconds = Math.floorDiv(value, 1_000_000_000);
          return Instant.ofEpochSecond(epochSeconds, value - (epochSeconds * 1_000_000_000));
        }
      };

  public static Int64Type<?> create(final LogicalType logicalType) {
    if (logicalType != null) {
      if (logicalType.isSetTIME()) {
        if (logicalType.getTIME().unit.isSetMILLIS()) {
          return TIME_MILLIS;
        }
        if (logicalType.getTIME().unit.isSetMICROS()) {
          return TIME_MICROS;
        }
        if (logicalType.getTIME().unit.isSetNANOS()) {
          return TIME_NANOS;
        }
      }
      if (logicalType.isSetTIMESTAMP()) {
        if (logicalType.getTIMESTAMP().unit.isSetMILLIS()) {
          return TIMESTAMP_MILLIS;
        }
        if (logicalType.getTIMESTAMP().unit.isSetMICROS()) {
          return TIMESTAMP_MICROS;
        }
        if (logicalType.getTIMESTAMP().unit.isSetNANOS()) {
          return TIMESTAMP_NANOS;
        }
      }
    }

    return LONGS;
  }
}
