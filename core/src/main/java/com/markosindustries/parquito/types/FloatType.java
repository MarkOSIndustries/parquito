package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.format.LogicalType;

public abstract class FloatType<ReadAs> extends ParquetType<ReadAs> {
  public FloatType(final Class<ReadAs> readAsClass) {
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
      throw new EOFException("Not enough bytes to read " + expectedValues + " Floats");
    }

    final var floatBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    return index -> wrap(floatBuffer.get(index));
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(0));
  }

  protected abstract ReadAs wrap(final float value);

  private static final FloatType<Float> FLOATS =
      new FloatType<Float>(Float.class) {
        @Override
        protected Float wrap(final float value) {
          return value;
        }

        @Override
        public int compare(final Float o1, final Float o2) {
          return o1.compareTo(o2);
        }
      };

  public static FloatType<?> create(final LogicalType logicalType) {
    return FLOATS;
  }
}
