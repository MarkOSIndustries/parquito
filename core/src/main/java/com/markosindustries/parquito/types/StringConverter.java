package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.parquet.format.Type;

public class StringConverter extends AbstractLogicalTypeConverter<String> {
  public static final StringConverter VARIABLE_LENGTH = new StringConverter(Type.BYTE_ARRAY);
  public static final StringConverter FIXED_LENGTH = new StringConverter(Type.FIXED_LEN_BYTE_ARRAY);

  private final Type type;

  public StringConverter(final Type type) {
    super(String.class);
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String fromByteBuffer(final ByteBuffer value) {
    final var bytes = new byte[value.remaining()];
    value.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public ByteBuffer toByteBuffer(final String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }
}
