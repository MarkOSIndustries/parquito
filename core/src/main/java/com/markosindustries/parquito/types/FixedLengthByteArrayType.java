package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.parquet.format.LogicalType;

public abstract class FixedLengthByteArrayType<ReadAs> extends ParquetType<ReadAs> {
  private final int typeLength;

  public FixedLengthByteArrayType(final Class<ReadAs> readAsClass, int typeLength) {
    super(readAsClass);
    this.typeLength = typeLength;
  }

  @Override
  public Values<ReadAs> readPlainPage(
      final int expectedValues, final int decompressedPageBytes, final InputStream inputStream)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var totalBytes = expectedValues * typeLength;
    final var values = ByteBuffer.allocate(totalBytes);
    inputStream.readNBytes(values.array(), 0, totalBytes);
    return index -> wrap(values.slice(index * typeLength, typeLength));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer buffer) {
    return wrap(buffer);
  }

  protected abstract ReadAs wrap(final ByteBuffer bytes);

  private static final class ByteBuffers extends FixedLengthByteArrayType<ByteBuffer> {
    public ByteBuffers(final int typeLength) {
      super(ByteBuffer.class, typeLength);
    }

    @Override
    protected ByteBuffer wrap(final ByteBuffer bytes) {
      return bytes;
    }

    @Override
    public int compare(final ByteBuffer o1, final ByteBuffer o2) {
      return ByteArrayType.unsignedByteComparison(o1, o2);
    }
  }

  private static final FixedLengthByteArrayType<UUID> UUIDS =
      new FixedLengthByteArrayType<UUID>(UUID.class, 16) {
        @Override
        protected UUID wrap(final ByteBuffer bytes) {
          long msb = 0, lsb = 0;
          for (int i = 0; i < 8; i++) msb = (msb << 8) | (bytes.get(i) & 0xff);
          for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (bytes.get(i) & 0xff);
          return new UUID(msb, lsb);
        }

        @Override
        public int compare(final UUID o1, final UUID o2) {
          final int topCmp =
              Int64Type.unsignedLongComparison(
                  o1.getMostSignificantBits(), o2.getMostSignificantBits());
          if (topCmp != 0) {
            return topCmp;
          }
          return Int64Type.unsignedLongComparison(
              o1.getLeastSignificantBits(), o2.getLeastSignificantBits());
        }
      };

  public static FixedLengthByteArrayType<?> create(
      final LogicalType logicalType, final int typeLength) {
    if (logicalType != null) {
      if (logicalType.isSetUUID()) {
        if (typeLength != 16) {
          throw new IllegalArgumentException(
              "UUID must use 16 bit fixed length byte arrays - but was " + typeLength);
        }
        return UUIDS;
      }
    }

    return new ByteBuffers(typeLength);
  }
}
