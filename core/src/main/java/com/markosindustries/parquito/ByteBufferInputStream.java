package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer byteBuffer;

  public ByteBufferInputStream(final ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public ByteBufferInputStream slice(int len) {
    return new ByteBufferInputStream(byteBuffer.slice(byteBuffer.position(), len));
  }

  public ByteBuffer readAsBufferView(int len) {
    final var result = byteBuffer.slice(byteBuffer.position(), len);
    byteBuffer.position(byteBuffer.position() + len);
    return result;
  }

  public int read() {
    if (byteBuffer.position() < byteBuffer.limit()) return 0xFF & byteBuffer.get();
    return -1;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (available() == 0) {
      return -1;
    }
    final var bytes = Math.min(available(), len);
    byteBuffer.get(b, off, bytes);
    return bytes;
  }

  @Override
  public byte[] readNBytes(final int len) throws IOException {
    final var result = new byte[Math.min(available(), len)];
    byteBuffer.get(result);
    return result;
  }

  @Override
  public long skip(final long n) throws IOException {
    final var skipBytes = Math.min((int) n, available());
    if (skipBytes == 0) {
      return -1;
    }
    byteBuffer.position(byteBuffer.position() + skipBytes);
    return skipBytes;
  }

  @Override
  public int available() throws IOException {
    return byteBuffer.limit() - byteBuffer.position();
  }

  @Override
  public synchronized void mark(final int readlimit) {
    byteBuffer.mark();
  }

  @Override
  public synchronized void reset() throws IOException {
    byteBuffer.reset();
  }

  @Override
  public boolean markSupported() {
    return true;
  }
}
