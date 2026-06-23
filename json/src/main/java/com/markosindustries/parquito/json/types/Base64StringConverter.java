package com.markosindustries.parquito.json.types;

import com.markosindustries.parquito.types.AbstractLogicalTypeConverter;
import com.markosindustries.parquito.types.ByteBufferConverter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.parquet.format.Type;

public class Base64StringConverter extends AbstractLogicalTypeConverter<String> {
  public static final ByteBufferConverter VARIABLE_LENGTH =
      new ByteBufferConverter(Type.BYTE_ARRAY);
  public static final ByteBufferConverter FIXED_LENGTH =
      new ByteBufferConverter(Type.FIXED_LEN_BYTE_ARRAY);

  private final Type type;

  public Base64StringConverter(final Type type) {
    super(String.class);
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String fromByteBuffer(final ByteBuffer value) {
    return StandardCharsets.UTF_8
        .decode(Base64.getEncoder().encode(value.asReadOnlyBuffer()))
        .toString();
  }

  @Override
  public ByteBuffer toByteBuffer(final String value) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
  }
}
