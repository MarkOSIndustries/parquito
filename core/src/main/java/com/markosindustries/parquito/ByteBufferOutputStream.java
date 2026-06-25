package com.markosindustries.parquito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferOutputStream extends ByteArrayOutputStream implements WritableByteChannel {
  public ByteBufferOutputStream() {}

  public ByteBufferOutputStream(final int size) {
    super(size);
  }

  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(buf, 0, count);
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    if (src.hasArray()) {
      write(src.array());
      return src.array().length;
    }
    final var buf = new byte[src.remaining()];
    src.get(buf);
    write(buf);
    return buf.length;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  public void clear() {
    count = 0;
  }
}
