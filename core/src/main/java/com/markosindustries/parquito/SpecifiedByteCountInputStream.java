package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;

public class SpecifiedByteCountInputStream extends InputStream {
  private final InputStream streamToReadFrom;
  private int bytesRemaining;

  public SpecifiedByteCountInputStream(InputStream streamToReadFrom, int bytesToRead) {
    this.streamToReadFrom = streamToReadFrom;
    this.bytesRemaining = bytesToRead;
  }

  @Override
  public int read() throws IOException {
    if ((bytesRemaining--) == 0) {
      return -1;
    }
    return streamToReadFrom.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (bytesRemaining == 0) {
      return -1;
    }
    final var bytesToRead = Math.min(bytesRemaining, len);
    bytesRemaining -= bytesToRead;
    return streamToReadFrom.read(b, off, bytesToRead);
  }
}
