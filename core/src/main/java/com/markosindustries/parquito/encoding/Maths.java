package com.markosindustries.parquito.encoding;

public class Maths {
  public static byte byteMaskUpperBits(int bits) {
    return (byte) (~(0xFF >> bits) & 0xFF);
  }

  public static byte byteMaskLowerBits(int bits) {
    return (byte) (~(0xFF << bits) & 0xFF);
  }

  public static int intMaskLowerBits(int bits) {
    return (int) (~(0xFFFFFFFFL << bits) & 0xFFFFFFFFL);
  }

  public static long longMaskLowerBits(int bits) {
    return bits == Maths.BITS_PER_LONG ? 0xFFFFFFFFFFFFFFFFL : ~(0xFFFFFFFFFFFFFFFFL << bits);
  }

  public static int floorDivPow2(int dividend, int divisorPowerOf2) {
    return dividend >>> divisorPowerOf2;
  }

  public static long floorDivPow2(long dividend, int divisorPowerOf2) {
    return dividend >>> divisorPowerOf2;
  }

  public static int ceilDivPow2(int dividend, int divisorPowerOf2) {
    int d = dividend >>> divisorPowerOf2;
    if ((d << divisorPowerOf2) != dividend) {
      return d + 1;
    }
    return d;
  }

  public static long ceilDivPow2(long dividend, int divisorPowerOf2) {
    long d = dividend >>> divisorPowerOf2;
    if ((d << divisorPowerOf2) != dividend) {
      return d + 1L;
    }
    return d;
  }

  public static int remainderDivPow2(int dividend, int divisorPowerOf2) {
    return dividend & ((1 << divisorPowerOf2) - 1);
  }

  public static long remainderDivPow2(long dividend, int divisorPowerOf2) {
    return dividend & ((1L << divisorPowerOf2) - 1);
  }

  public static int bitWidth(int value) {
    return BITS_PER_INT - Integer.numberOfLeadingZeros(value);
  }

  public static int bitWidth(long value) {
    return BITS_PER_LONG - Long.numberOfLeadingZeros(value);
  }

  public static int nextPowerOfTwo(int value) {
    return 1 << (bitWidth(value) + 1);
  }

  public static long nextPowerOfTwo(long value) {
    return 1L << (bitWidth(value) + 1);
  }

  public static final byte BYTES_PER_SHORT = 2;
  public static final byte BYTES_PER_INT = 4;
  public static final byte BYTES_PER_LONG = 8;

  public static final byte BITS_PER_BYTE = 8;
  public static final byte BITS_PER_SHORT = BYTES_PER_SHORT * BITS_PER_BYTE;
  public static final byte BITS_PER_INT = BYTES_PER_INT * BITS_PER_BYTE;
  public static final byte BITS_PER_LONG = BYTES_PER_LONG * BITS_PER_BYTE;
}
