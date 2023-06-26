package com.markosindustries.parquito.encoding;

public interface ZigZag {
  static int encode(final int value) {
    int sign = value < 0 ? -1 : 0;
    return (value << 1) ^ sign;
  }

  static int decode(final int value) {
    return (value ^ -(value & 1)) >> 1;
  }

  static long encode(final long value) {
    int sign = value < 0 ? -1 : 0;
    return (value << 1) ^ sign;
  }

  static long decode(final long value) {
    return (value ^ -(value & 1)) >> 1;
  }
}
