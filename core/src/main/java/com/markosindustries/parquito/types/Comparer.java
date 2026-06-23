package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.format.LogicalType;

public sealed interface Comparer permits Comparer.Signed, Comparer.Unsigned {
  int compareBoolean(boolean value1, boolean value2);

  int compareByteBuffer(ByteBuffer value1, ByteBuffer value2);

  int compareFloat(float value1, float value2);

  int compareDouble(double value1, double value2);

  int compareInt32(int value1, int value2);

  int compareInt64(long value1, long value2);

  final class Signed implements Comparer {
    @Override
    public int compareBoolean(final boolean value1, final boolean value2) {
      return Boolean.compare(value1, value2);
    }

    @Override
    public int compareByteBuffer(final ByteBuffer value1, final ByteBuffer value2) {
      return unsignedByteComparison(value1, value2);
    }

    @Override
    public int compareDouble(final double value1, final double value2) {
      return Double.compare(value1, value2);
    }

    @Override
    public int compareFloat(final float value1, final float value2) {
      return Float.compare(value1, value2);
    }

    @Override
    public int compareInt32(final int value1, final int value2) {
      return Integer.compare(value1, value2);
    }

    @Override
    public int compareInt64(final long value1, final long value2) {
      return Long.compare(value1, value2);
    }
  }

  final class Unsigned implements Comparer {
    @Override
    public int compareBoolean(final boolean value1, final boolean value2) {
      return Boolean.compare(value1, value2);
    }

    @Override
    public int compareByteBuffer(final ByteBuffer value1, final ByteBuffer value2) {
      return unsignedByteComparison(value1, value2);
    }

    @Override
    public int compareDouble(final double value1, final double value2) {
      return Double.compare(value1, value2);
    }

    @Override
    public int compareFloat(final float value1, final float value2) {
      return Float.compare(value1, value2);
    }

    @Override
    public int compareInt32(final int value1, final int value2) {
      return Integer.compareUnsigned(value1, value2);
    }

    @Override
    public int compareInt64(final long value1, final long value2) {
      return Long.compareUnsigned(value1, value2);
    }
  }

  private static int unsignedByteComparison(final ByteBuffer o1, final ByteBuffer o2) {
    if (o1.hasArray() && o2.hasArray()) {
      return Arrays.compareUnsigned(
          o1.array(),
          o1.arrayOffset() + o1.position(),
          o1.arrayOffset() + o1.limit(),
          o2.array(),
          o2.arrayOffset() + o2.position(),
          o2.arrayOffset() + o2.limit());
    }

    final int o1Start = o1.position();
    final int o1Size = o1.limit() - o1Start;
    final int o2Start = o2.position();
    final int o2Size = o2.limit() - o2Start;
    int cmp;
    for (int o1Index = o1Start, o2Index = o2Start;
        o1Index < o1.limit() && o2Index < o2.limit();
        o1Index++, o2Index++) {
      cmp = Byte.compareUnsigned(o1.get(o1Index), o2.get(o2Index));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(o1Size, o2Size);
  }

  Signed SIGNED = new Signed();
  Unsigned UNSIGNED = new Unsigned();

  static Comparer comparerFor(final LogicalType logicalType) {
    return logicalType == null || !logicalType.isSetINTEGER() || logicalType.getINTEGER().isSigned
        ? SIGNED
        : UNSIGNED;
  }
}
