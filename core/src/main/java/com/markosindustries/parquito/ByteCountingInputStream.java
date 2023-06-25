package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteCountingInputStream extends InputStream {
  private final InputStream inputStream;
  private int bytesRead;

  public ByteCountingInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
    this.bytesRead = 0;
  }

  public int getBytesRead() {
    return bytesRead;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    final var read = inputStream.read(b);
    if (read > -1) {
      bytesRead += read;
    }
    return read;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    final var read = inputStream.read(b, off, len);
    if (read > -1) {
      bytesRead += read;
    }
    return read;
  }

  @Override
  public byte[] readAllBytes() throws IOException {
    final var read = inputStream.readAllBytes();
    bytesRead += read.length;
    return read;
  }

  @Override
  public byte[] readNBytes(final int len) throws IOException {
    final var read = inputStream.readNBytes(len);
    bytesRead += read.length;
    return read;
  }

  @Override
  public int readNBytes(final byte[] b, final int off, final int len) throws IOException {
    final var read = inputStream.readNBytes(b, off, len);
    bytesRead += read;
    return read;
  }

  @Override
  public long skip(final long n) throws IOException {
    final var read = inputStream.skip(n);
    bytesRead += read;
    return read;
  }

  @Override
  public void skipNBytes(final long n) throws IOException {
    inputStream.skipNBytes(n);
    bytesRead += n;
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public synchronized void mark(final int readlimit) {
    super.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    super.reset();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    final var read = inputStream.transferTo(out);
    bytesRead += read;
    return read;
  }

  @Override
  public int read() throws IOException {
    final var read = inputStream.read();
    if (read > -1) {
      bytesRead++;
    }
    return read;
  }
}
