package com.markosindustries.parquito.filesys;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

public class ByteBufferBasedSeekableInputStream extends SeekableInputStream {
  private final ByteBuffer parquetFileBuffer;

  public ByteBufferBasedSeekableInputStream(final ByteBuffer parquetFileBuffer) {
    this.parquetFileBuffer = parquetFileBuffer;
  }

  @Override
  public long getPos() throws IOException {
    return parquetFileBuffer.position();
  }

  @Override
  public void seek(final long newPos) throws IOException {
    parquetFileBuffer.position((int) newPos);
  }

  @Override
  public void readFully(final byte[] bytes) throws IOException {
    parquetFileBuffer.get(bytes);
  }

  @Override
  public void readFully(final byte[] bytes, final int start, final int len) throws IOException {
    parquetFileBuffer.get(bytes, start, len);
  }

  @Override
  public int read(final ByteBuffer buf) throws IOException {
    final var bytesToTransfer = Math.min(parquetFileBuffer.remaining(), buf.remaining());
    buf.put(buf.position(), parquetFileBuffer, parquetFileBuffer.position(), bytesToTransfer);
    buf.position(buf.position() + bytesToTransfer);
    return bytesToTransfer;
  }

  @Override
  public void readFully(final ByteBuffer buf) throws IOException {
    final var bytesToTransfer = buf.remaining();
    buf.put(buf.position(), parquetFileBuffer, parquetFileBuffer.position(), bytesToTransfer);
    buf.position(buf.position() + bytesToTransfer);
  }

  @Override
  public int read() throws IOException {
    return parquetFileBuffer.get() & 0xFF;
  }
}
