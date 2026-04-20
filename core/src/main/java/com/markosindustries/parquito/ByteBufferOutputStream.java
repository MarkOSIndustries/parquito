package com.markosindustries.parquito;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends ByteArrayOutputStream {
  public ByteBufferOutputStream() {}

  public ByteBufferOutputStream(final int size) {
    super(size);
  }

  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(buf, 0, count).asReadOnlyBuffer();
  }
}
