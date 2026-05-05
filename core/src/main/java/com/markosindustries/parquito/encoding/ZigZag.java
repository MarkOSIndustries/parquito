package com.markosindustries.parquito.encoding;

public interface ZigZag {
  static int encode(final int value) {
    return (value << 1) ^ (value >> 31);
  }

  static int decode(final int value) {
    return (value >>> 1) ^ -(value & 1);
  }

  static long encode(final long value) {
    return (value << 1) ^ (value >> 63);
  }

  static long decode(final long value) {
    return (value >>> 1) ^ -(value & 1L);
  }
}
