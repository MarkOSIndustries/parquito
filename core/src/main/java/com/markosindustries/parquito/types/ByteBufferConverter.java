package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import org.apache.parquet.format.Type;

public class ByteBufferConverter extends AbstractLogicalTypeConverter<ByteBuffer> {
  public static final ByteBufferConverter VARIABLE_LENGTH =
      new ByteBufferConverter(Type.BYTE_ARRAY);
  public static final ByteBufferConverter FIXED_LENGTH =
      new ByteBufferConverter(Type.FIXED_LEN_BYTE_ARRAY);

  private final Type type;

  public ByteBufferConverter(final Type type) {
    super(ByteBuffer.class);
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public ByteBuffer fromByteBuffer(final ByteBuffer value) {
    return value.asReadOnlyBuffer();
  }

  @Override
  public ByteBuffer toByteBuffer(final ByteBuffer value) {
    return value.asReadOnlyBuffer();
  }
}
