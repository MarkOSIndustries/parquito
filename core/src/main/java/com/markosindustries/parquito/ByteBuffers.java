package com.markosindustries.parquito;

import java.nio.ByteBuffer;
import java.util.HexFormat;

public class ByteBuffers {
  public static String hexView(final ByteBuffer buf) {
    var bytes = new byte[buf.limit()];
    buf.get(0, bytes);
    return HexFormat.of().formatHex(bytes);
  }
}
