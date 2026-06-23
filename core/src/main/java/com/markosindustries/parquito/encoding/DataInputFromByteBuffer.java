package com.markosindustries.parquito.encoding;

import java.io.DataInput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

class DataInputFromByteBuffer implements DataInput {
  private final ByteBuffer byteBuffer;

  public DataInputFromByteBuffer(final ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public void readFully(final byte[] b) throws IOException {
    byteBuffer.get(b);
  }

  @Override
  public void readFully(final byte[] b, final int off, final int len) throws IOException {
    byteBuffer.get(b, off, len);
  }

  @Override
  public int skipBytes(final int n) throws IOException {
    final var skippable = Math.min(byteBuffer.remaining(), n);
    byteBuffer.position(skippable);
    return skippable;
  }

  @Override
  public boolean readBoolean() throws IOException {
    throw new UnsupportedEncodingException("Not expecting to read booleans");
  }

  @Override
  public byte readByte() throws IOException {
    return byteBuffer.get();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return 0xFF & byteBuffer.get();
  }

  @Override
  public short readShort() throws IOException {
    return byteBuffer.getShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return 0xFFFF & byteBuffer.getShort();
  }

  @Override
  public char readChar() throws IOException {
    throw new UnsupportedEncodingException("Not expecting to read chars");
  }

  @Override
  public int readInt() throws IOException {
    return byteBuffer.getInt();
  }

  @Override
  public long readLong() throws IOException {
    return byteBuffer.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    return byteBuffer.getFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return byteBuffer.getDouble();
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedEncodingException("Not expecting to read lines");
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedEncodingException("Not expecting to read Strings");
  }
}
