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

public abstract class Int32Type<ReadAs> extends ParquetType<ReadAs> {
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

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var requiredBytes = values.size() * 4;
    final var buffer = ByteBuffer.allocate(requiredBytes);
    final var intBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    intBuffer.position(0);
    for (final var value : values) {
      intBuffer.put(unwrap(value));
    }
    outputStream.write(buffer.array());
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return 4;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(unwrap(value));
  }

  protected abstract ReadAs wrap(final int value);

  protected abstract int unwrap(final ReadAs value);

  private static final Int32Type<Integer> SIGNED_INTEGERS =
      new Int32Type<Integer>(Integer.class) {
        @Override
        protected Integer wrap(final int value) {
          return value;
        }

        @Override
        protected int unwrap(final Integer value) {
          return value;
        }

        @Override
        public int compare(final Integer o1, final Integer o2) {
          return o1.compareTo(o2);
        }
      };
  private static final Int32Type<Integer> UNSIGNED_INTEGERS =
      new Int32Type<Integer>(Integer.class) {
        @Override
        protected Integer wrap(final int value) {
          return value;
        }

        @Override
        protected int unwrap(final Integer value) {
          return value;
        }

        @Override
        public int compare(final Integer o1, final Integer o2) {
          return Long.compare(0xFFFFFFFFL & o1, 0xFFFFFFFFL & o2);
        }
      };
  private static final Int32Type<Instant> DATES =
      new Int32Type<Instant>(Instant.class) {
        @Override
        protected Instant wrap(final int value) {
          return Instant.ofEpochSecond(Duration.ofDays(value).getSeconds());
        }

        @Override
        protected int unwrap(final Instant value) {
          return (int) value.getEpochSecond();
        }

        @Override
        public int compare(final Instant o1, final Instant o2) {
          return o1.compareTo(o2);
        }
      };

  public static Int32Type<?> create(final LogicalType logicalType) {
    if (logicalType != null) {
      if (logicalType.isSetDATE()) {
        return DATES;
      }

      if (logicalType.isSetINTEGER()) {
        if (logicalType.getINTEGER().isSigned) {
          return SIGNED_INTEGERS;
        } else {
          return UNSIGNED_INTEGERS;
        }
      }
    }

    return SIGNED_INTEGERS;
  }
}
