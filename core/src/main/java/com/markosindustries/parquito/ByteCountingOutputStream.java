package com.markosindustries.parquito;

import java.io.IOException;
import java.io.OutputStream;

public class ByteCountingOutputStream extends OutputStream {
  private final OutputStream outputStream;
  private int bytesWritten;

  public ByteCountingOutputStream(final OutputStream outputStream) {
    this.outputStream = outputStream;
    this.bytesWritten = 0;
  }

  public int getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public void write(final int b) throws IOException {
    outputStream.write(b);
    bytesWritten++;
  }

  @Override
  public void write(final byte[] b) throws IOException {
    outputStream.write(b);
    bytesWritten += b.length;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    outputStream.write(b, off, len);
    bytesWritten += len;
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
