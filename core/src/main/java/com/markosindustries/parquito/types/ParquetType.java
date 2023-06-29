package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.Type;

public abstract class ParquetType<ReadAs> {
  protected final Class<ReadAs> readAsClass;

  protected ParquetType(final Class<ReadAs> readAsClass) {
    this.readAsClass = readAsClass;
  }

  public final Class<ReadAs> getReadAsClass() {
    return readAsClass;
  }

  public abstract Values<ReadAs> readPlainPage(
      final int expectedValues, final int decompressedPageBytes, final InputStream inputStream)
      throws IOException;

  public abstract ReadAs readFromByteBuffer(final ByteBuffer byteBuffer);

  /**
   * We don't implement {@link java.util.Comparator} because we have no need of Serialization etc.
   *
   * @see java.util.Comparator
   * @param o1 Comparable left
   * @param o2 Comparable right
   * @return &lt;0 if o1&lt;o2, 0 if o1==o2, &gt;0 if o1&gt;o2
   */
  public abstract int compare(ReadAs o1, ReadAs o2);

  public static ParquetType<?> create(
      final Type type, final LogicalType logicalType, final int typeLength) {
    return switch (type) {
      case BOOLEAN -> BooleanType.create(logicalType);
      case INT32 -> Int32Type.create(logicalType);
      case INT64 -> Int64Type.create(logicalType);
        //      case INT96 -> null; // TODO: throwing for now...
      case FLOAT -> FloatType.create(logicalType);
      case DOUBLE -> DoubleType.create(logicalType);
      case BYTE_ARRAY -> ByteArrayType.create(logicalType);
      case FIXED_LEN_BYTE_ARRAY -> FixedLengthByteArrayType.create(logicalType, typeLength);

      default -> throw new IllegalArgumentException("Can't currently read values of type " + type);
    };
  }
}
