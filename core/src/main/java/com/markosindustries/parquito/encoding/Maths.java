package com.markosindustries.parquito.encoding;

public class Maths {
  static int intMaskLowerBits(int bits) {
    return ~(0xFFFFFFFF << bits);
  }

  static long longMaskLowerBits(int bits) {
    return ~(0xFFFFFFFFFFFFFFFFL << bits);
  }

  static int floorDivPow2(int dividend, int divisorPowerOf2) {
    return dividend >>> divisorPowerOf2;
  }

  static long floorDivPow2(long dividend, int divisorPowerOf2) {
    return dividend >>> divisorPowerOf2;
  }

  static int ceilDivPow2(int dividend, int divisorPowerOf2) {
    int d = dividend >>> divisorPowerOf2;
    if ((d << divisorPowerOf2) != dividend) {
      return d + 1;
    }
    return d;
  }

  static long ceilDivPow2(long dividend, int divisorPowerOf2) {
    long d = dividend >>> divisorPowerOf2;
    if ((d << divisorPowerOf2) != dividend) {
      return d + 1L;
    }
    return d;
  }

  static int remainderDivPow2(int dividend, int divisorPowerOf2) {
    return dividend & ((1 << divisorPowerOf2) - 1);
  }

  static long remainderDivPow2(long dividend, int divisorPowerOf2) {
    return dividend & ((1L << divisorPowerOf2) - 1);
  }

  public static int bitWidth(int value) {
    return BITS_PER_INT - Integer.numberOfLeadingZeros(value);
  }

  public static int bitWidth(long value) {
    return BITS_PER_LONG - Long.numberOfLeadingZeros(value);
  }

  public static final byte BITS_PER_BYTE = 8;
  public static final byte BITS_PER_INT = 32;
  public static final byte BITS_PER_LONG = 64;
}
