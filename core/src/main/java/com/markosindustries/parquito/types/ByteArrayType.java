package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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

    final var lengthPrefixBuffer = new byte[4];
    final var lengthPrefix =
        ByteBuffer.wrap(lengthPrefixBuffer).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    final var indices = new int[expectedValues];
    final var sizes = new int[expectedValues];
    final var values = ByteBuffer.allocate(decompressedPageBytes - (expectedValues * 4));
    for (int index = 0, i = 0; i < expectedValues; i++) {
      if (inputStream.read(lengthPrefixBuffer) != 4) {
        throw new EOFException();
      }
      final var size = lengthPrefix.get(0);
      indices[i] = index;
      sizes[i] = size;
      if (size > 0) {
        inputStream.readNBytes(values.array(), index, size);
      }
      index += size;
    }

    return index ->
        sizes[index] == 0 ? emptyValue() : wrap(values.slice(indices[index], sizes[index]));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer buffer) {
    return wrap(buffer);
  }

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  protected ReadAs emptyValue() {
    return wrap(EMPTY_BUFFER);
  }

  protected abstract ReadAs wrap(final ByteBuffer bytes);

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

  private static final ByteArrayType<ByteBuffer> BYTE_BUFFERS =
      new ByteArrayType<ByteBuffer>(ByteBuffer.class) {
        @Override
        protected ByteBuffer wrap(final ByteBuffer bytes) {
          return bytes;
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
        public int compare(final String o1, final String o2) {
          return ByteArrayType.unsignedByteComparison(
              ByteBuffer.wrap(o1.getBytes(StandardCharsets.UTF_8)),
              ByteBuffer.wrap(o2.getBytes(StandardCharsets.UTF_8)));
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
