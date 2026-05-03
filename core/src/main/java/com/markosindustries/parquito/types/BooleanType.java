package com.markosindustries.parquito.types;

import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import org.apache.parquet.format.LogicalType;

public abstract class BooleanType<ReadAs> extends ParquetType<ReadAs> {
  public BooleanType(final Class<ReadAs> booleanClass) {
    super(booleanClass);
  }

  @Override
  public Values<ReadAs> readPlainPage(
      int expectedValues, final int decompressedPageBytes, InputStream inputStream)
      throws IOException {
    final int[] values = IntEncodings.INT_ENCODING_RLE.decode(expectedValues, 1, inputStream);
    return index -> wrap(values[index] != 0);
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer buffer) {
    return wrap(buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(0) != 0);
  }

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var requiredBytes = values.size() * 4;
    final var buffer = ByteBuffer.allocate(requiredBytes);
    final var intBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    intBuffer.position(0);
    for (final var value : values) {
      intBuffer.put(unwrap(value) ? 1 : 0);
    }
    outputStream.write(buffer.array());
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return 4;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(unwrap(value) ? 1 : 0);
  }

  protected abstract ReadAs wrap(final boolean value);

  protected abstract boolean unwrap(final ReadAs value);

  private static final BooleanType<Boolean> BOOLEANS =
      new BooleanType<Boolean>(Boolean.class) {
        @Override
        protected Boolean wrap(final boolean value) {
          return value;
        }

        protected boolean unwrap(final Boolean value) {
          return value;
        }

        @Override
        public int compare(final Boolean o1, final Boolean o2) {
          return o1.compareTo(o2);
        }
      };

  public static BooleanType<?> create(final LogicalType logicalType) {
    return BOOLEANS;
  }
}
