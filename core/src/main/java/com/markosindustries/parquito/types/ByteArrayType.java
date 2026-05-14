package com.markosindustries.parquito.types;

import com.markosindustries.parquito.encoding.LittleEndian;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.apache.parquet.format.LogicalType;

public abstract class ByteArrayType<ReadAs> extends ParquetType<ReadAs> {
  public ByteArrayType(final Class<ReadAs> readAsClass) {
    super(readAsClass);
  }

  @Override
  public Values<ReadAs> readPlainPage(
      final int expectedValues, final int decompressedPageBytes, final InputStream inputStream)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var bytesRequired = decompressedPageBytes - (expectedValues * 4);
    if (bytesRequired == 0) {
      return Values.repeated(emptyValue(), expectedValues);
    }

    final var indices = new int[expectedValues];
    final var sizes = new int[expectedValues];
    final var values = ByteBuffer.allocate(decompressedPageBytes - (expectedValues * 4));
    for (int index = 0, i = 0; i < expectedValues; i++) {
      final var size = LittleEndian.readInt(inputStream);
      indices[i] = index;
      sizes[i] = size;
      if (size > 0) {
        inputStream.readNBytes(values.array(), index, size);
      }
      index += size;
    }

    return new Values<ReadAs>() {
      @Override
      public ReadAs get(final int index) {
        return sizes[index] == 0 ? emptyValue() : wrap(values.slice(indices[index], sizes[index]));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer buffer) {
    return wrap(buffer);
  }

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var writableChannel = Channels.newChannel(outputStream);

    for (final var value : values) {
      final var unwrapped = unwrap(value);
      LittleEndian.writeInt(unwrapped.remaining(), outputStream);
      writableChannel.write(unwrapped);
    }
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return unwrap(value).remaining();
  }

  @Override
  public int getPlainBytesOverhead() {
    return 4;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.put(unwrap(value));
  }

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  protected ReadAs emptyValue() {
    return wrap(EMPTY_BUFFER);
  }

  protected abstract ReadAs wrap(final ByteBuffer bytes);

  protected abstract ByteBuffer unwrap(final ReadAs value);

  static int unsignedByteComparison(final ByteBuffer o1, final ByteBuffer o2) {
    final int o1Start = o1.position();
    final int o1Size = o1.limit() - o1Start;
    final int o2Start = o2.position();
    final int o2Size = o2.limit() - o2Start;
    final int length = Math.min(o1Size, o2Size);
    int cmp;
    for (int i = 0; i < length; i++) {
      cmp = Byte.compareUnsigned(o1.get(o1Start + i), o2.get(o2Start + i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(o1Size, o2Size);
  }

  static int unsignedByteComparison(final byte[] o1, final byte[] o2) {
    final int length = Math.min(o1.length, o2.length);
    int cmp;
    for (int i = 0; i < length; i++) {
      cmp = Byte.compareUnsigned(o1[i], o2[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(o1.length, o2.length);
  }

  private static final ByteArrayType<ByteBuffer> BYTE_BUFFERS =
      new ByteArrayType<ByteBuffer>(ByteBuffer.class) {
        @Override
        protected ByteBuffer wrap(final ByteBuffer bytes) {
          return bytes.asReadOnlyBuffer();
        }

        @Override
        protected ByteBuffer unwrap(final ByteBuffer value) {
          return value.asReadOnlyBuffer();
        }

        @Override
        public int compare(final ByteBuffer o1, final ByteBuffer o2) {
          return ByteArrayType.unsignedByteComparison(o1, o2);
        }
      };

  private static final ByteArrayType<String> STRINGS =
      new ByteArrayType<String>(String.class) {
        @Override
        protected String emptyValue() {
          return "";
        }

        @Override
        protected String wrap(final ByteBuffer bytes) {
          return new String(
              bytes.array(), bytes.arrayOffset(), bytes.capacity(), StandardCharsets.UTF_8);
        }

        @Override
        protected ByteBuffer unwrap(final String value) {
          return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public int compare(final String o1, final String o2) {
          return ByteArrayType.unsignedByteComparison(
              o1.getBytes(StandardCharsets.UTF_8), o2.getBytes(StandardCharsets.UTF_8));
        }
      };

  public static ByteArrayType<?> create(final LogicalType logicalType) {
    if (logicalType != null) {
      if (logicalType.isSetSTRING() || logicalType.isSetENUM() || logicalType.isSetJSON()) {
        return STRINGS;
      }
    }

    return BYTE_BUFFERS;
  }
}
