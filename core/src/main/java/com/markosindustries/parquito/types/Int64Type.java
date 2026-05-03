package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import org.apache.parquet.format.LogicalType;

public abstract class Int64Type<ReadAs> extends ParquetType<ReadAs> {
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

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var requiredBytes = values.size() * 8;
    final var buffer = ByteBuffer.allocate(requiredBytes);
    final var longBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    longBuffer.position(0);
    for (final var value : values) {
      longBuffer.put(unwrap(value));
    }
    outputStream.write(buffer.array());
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return 8;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(unwrap(value));
  }

  protected abstract ReadAs wrap(final long value);

  protected abstract long unwrap(final ReadAs value);

  static int unsignedLongComparison(final Long o1, final Long o2) {
    final int top63Cmp = Long.compare(o1 >>> 1, o2 >>> 1);
    if (top63Cmp != 0) {
      return top63Cmp;
    }
    return Long.compare(o1 & 1L, o2 & 1L);
  }

  private static final Int64Type<Long> SIGNED_LONGS =
      new Int64Type<Long>(Long.class) {
        @Override
        protected Long wrap(final long value) {
          return value;
        }

        @Override
        protected long unwrap(final Long value) {
          return value;
        }

        @Override
        public int compare(final Long o1, final Long o2) {
          return o1.compareTo(o2);
        }
      };

  private static final Int64Type<Long> UNSIGNED_LONGS =
      new Int64Type<Long>(Long.class) {
        @Override
        protected Long wrap(final long value) {
          return value;
        }

        @Override
        protected long unwrap(final Long value) {
          return value;
        }

        @Override
        public int compare(final Long o1, final Long o2) {
          return Int64Type.unsignedLongComparison(o1, o2);
        }
      };
  private static final Int64Type<Duration> TIME_MILLIS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofMillis(value);
        }

        @Override
        protected long unwrap(final Duration value) {
          return value.toMillis();
        }

        @Override
        public int compare(final Duration o1, final Duration o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int64Type<Duration> TIME_MICROS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofNanos(value * 1_000);
        }

        @Override
        protected long unwrap(final Duration value) {
          return (value.getSeconds() * 1_000_000L) + (Math.floorDiv(value.getNano(), 1_000L));
        }

        @Override
        public int compare(final Duration o1, final Duration o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int64Type<Duration> TIME_NANOS =
      new Int64Type<Duration>(Duration.class) {
        @Override
        protected Duration wrap(final long value) {
          return Duration.ofNanos(value);
        }

        @Override
        protected long unwrap(final Duration value) {
          return value.toNanos();
        }

        @Override
        public int compare(final Duration o1, final Duration o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_MILLIS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          return Instant.ofEpochMilli(value);
        }

        @Override
        protected long unwrap(final Instant value) {
          return value.toEpochMilli();
        }

        @Override
        public int compare(final Instant o1, final Instant o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_MICROS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          final var epochSeconds = Math.floorDiv(value, 1_000_000L);
          return Instant.ofEpochSecond(epochSeconds, value - (epochSeconds * 1_000_000L));
        }

        @Override
        protected long unwrap(final Instant value) {
          return (value.getEpochSecond() * 1_000_000L) + Math.floorDiv(value.getNano(), 1_000L);
        }

        @Override
        public int compare(final Instant o1, final Instant o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int64Type<Instant> TIMESTAMP_NANOS =
      new Int64Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final long value) {
          final var epochSeconds = Math.floorDiv(value, 1_000_000_000L);
          return Instant.ofEpochSecond(epochSeconds, value - (epochSeconds * 1_000_000_000L));
        }

        @Override
        protected long unwrap(final Instant value) {
          return (value.getEpochSecond() * 1_000_000_000L) + value.getNano();
        }

        @Override
        public int compare(final Instant o1, final Instant o2) {
          return o1.compareTo(o2);
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

      if (logicalType.isSetINTEGER()) {
        if (logicalType.getINTEGER().isSigned) {
          return SIGNED_LONGS;
        } else {
          return UNSIGNED_LONGS;
        }
      }
    }

    return SIGNED_LONGS;
  }
}
