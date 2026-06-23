package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class LongConverter extends AbstractLogicalTypeConverter<Long> {
  public static final LongConverter INSTANCE = new LongConverter();

  public LongConverter() {
    super(Long.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Long fromInt64(final long value) {
    return value;
  }

  @Override
  public long toInt64(final Long value) {
    return value;
  }
}
