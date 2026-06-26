package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ParquetIOException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface VarInt {
  static int sizeUnsigned32(int value) {
    int size = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      size++;
      value >>>= 7;
    }
    return size;
  }

  static void putUnsigned32(int value, final ByteBuffer target) {
    while ((value & 0xFFFFFF80) != 0L) {
      target.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    target.put((byte) (value & 0x7F));
  }

  static void putUnsigned32(int value, final OutputStream target) throws IOException {
    while ((value & 0xFFFFFF80) != 0L) {
      target.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    target.write(value & 0x7F);
  }

  static int sizeUnsigned64(long value) {
    int size = 1;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      size++;
      value >>>= 7;
    }
    return size;
  }

  static void putUnsigned64(long value, final ByteBuffer target) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      target.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    target.put((byte) (value & 0x7F));
  }

  static void putUnsigned64(long value, final OutputStream target) throws IOException {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      target.write((int) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    target.write((int) (value & 0x7F));
  }

  static int getUnsigned32(final ByteBuffer source) {
    int value = 0;
    int i = 0;
    int b;
    while (((b = source.get()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 35) {
        throw new ParquetIOException("Too many bytes for VarInt32");
      }
    }
    return value | (b << i);
  }

  static long getUnsigned64(final ByteBuffer source) {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = source.get()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63) {
        throw new ParquetIOException("Too many bytes for VarInt64");
      }
    }
    return value | (b << i);
  }
}
