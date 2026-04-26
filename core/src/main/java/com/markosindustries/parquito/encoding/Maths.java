package com.markosindustries.parquito.encoding;

public class Maths {
  public static int intMaskLowerBits(int bits) {
    return ~(0xFFFFFFFF << bits);
  }

  public static long longMaskLowerBits(int bits) {
    return ~(0xFFFFFFFFFFFFFFFFL << bits);
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

  public static final byte BYTES_PER_INT = 4;
  public static final byte BYTES_PER_LONG = 8;

  public static final byte BITS_PER_BYTE = 8;
  public static final byte BITS_PER_INT = BYTES_PER_INT * BITS_PER_BYTE;
  public static final byte BITS_PER_LONG = BYTES_PER_LONG * BITS_PER_BYTE;
}
