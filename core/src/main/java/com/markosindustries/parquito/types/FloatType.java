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
    if (inputStream.readNBytes(buffer.array(), 0, expectedBytes) != expectedBytes) {
      throw new EOFException("Not enough bytes to read " + expectedValues + " Floats");
    }

    final var floatBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();

    return new Values<ReadAs>() {
      @Override
      public ReadAs get(final int index) {
        return wrap(floatBuffer.get(index));
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  @Override
  public ReadAs readFromByteBuffer(final ByteBuffer byteBuffer) {
    return wrap(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(0));
  }

  @Override
  public void writePlainPage(final Collection<ReadAs> values, final OutputStream outputStream)
      throws IOException {
    final var requiredBytes = values.size() * 4;
    final var buffer = ByteBuffer.allocate(requiredBytes);
    final var floatBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    floatBuffer.position(0);
    for (final var value : values) {
      floatBuffer.put(unwrap(value));
    }
    outputStream.write(buffer.array());
  }

  @Override
  public int getRequiredBytesToWrite(final ReadAs value) {
    return 4;
  }

  @Override
  public void writeToByteBuffer(final ReadAs value, final ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(unwrap(value));
    byteBuffer.position(byteBuffer.position() + 4);
  }

  protected abstract ReadAs wrap(final float value);

  protected abstract float unwrap(final ReadAs value);

  private static final FloatType<Float> FLOATS =
      new FloatType<Float>(Float.class) {
        @Override
        protected Float wrap(final float value) {
          return value;
        }

        @Override
        protected float unwrap(final Float value) {
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
