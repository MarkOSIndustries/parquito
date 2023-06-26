package com.markosindustries.parquito.encoding;

public class Maths {
  static int intMaskLowerBits(int bits) {
    return 0xFFFFFFFF >>> (32 - bits);
  }

  static long longMaskLowerBits(int bits) {
    return 0xFFFFFFFFFFFFFFFFL >>> (64 - bits);
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
}
