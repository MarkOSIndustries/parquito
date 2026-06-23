package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.parquet.format.Type;

public class UUIDConverter extends AbstractLogicalTypeConverter<UUID> {
  public static final UUIDConverter INSTANCE = new UUIDConverter();

  public UUIDConverter() {
    super(UUID.class);
  }

  @Override
  public Type getType() {
    return Type.FIXED_LEN_BYTE_ARRAY;
  }

  @Override
  public UUID fromByteBuffer(final ByteBuffer value) {
    long msb = 0, lsb = 0;
    for (int i = 0; i < 8; i++) msb = (msb << 8) | (value.get(i) & 0xff);
    for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (value.get(i) & 0xff);
    return new UUID(msb, lsb);
  }

  @Override
  public ByteBuffer toByteBuffer(final UUID value) {
    final var buffer = ByteBuffer.allocate(16);
    long msb = value.getMostSignificantBits();
    long lsb = value.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      buffer.put(i, (byte) (msb & 0xff));
      msb = (msb >>> 8);
    }
    for (int i = 8; i < 16; i++) {
      buffer.put(i, (byte) (lsb & 0xff));
      lsb = (lsb >>> 8);
    }
    return buffer.flip();
  }
}
