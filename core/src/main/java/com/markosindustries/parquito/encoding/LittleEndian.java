package com.markosindustries.parquito.encoding;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public interface LittleEndian {
  static int readInt(InputStream inputStream) throws IOException {
    int byte1 = inputStream.read();
    int byte2 = inputStream.read();
    int byte3 = inputStream.read();
    int byte4 = inputStream.read();
    if ((byte1 | byte2 | byte3 | byte4) < 0) throw new EOFException();
    return (byte4 << 24) + (byte3 << 16) + (byte2 << 8) + byte1;
  }
}
