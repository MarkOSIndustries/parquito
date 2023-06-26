package com.markosindustries.parquito.types;

import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.format.LogicalType;

public abstract class BooleanType<ReadAs extends Comparable<ReadAs>> extends ParquetType<ReadAs> {
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
  public ReadAs readColumnStatsValue(final ByteBuffer buffer) {
    return wrap(buffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(0) != 0);
  }

  protected abstract ReadAs wrap(final boolean value);

  private static final BooleanType<Boolean> BOOLEANS =
      new BooleanType<Boolean>(Boolean.class) {
        @Override
        protected Boolean wrap(final boolean value) {
          return value;
        }
      };

  public static BooleanType<?> create(final LogicalType logicalType) {
    return BOOLEANS;
  }
}
