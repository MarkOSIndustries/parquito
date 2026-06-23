package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class UnsignedLongConverter extends AbstractLogicalTypeConverter<Long> {
  public static final UnsignedLongConverter INSTANCE = new UnsignedLongConverter();

  public UnsignedLongConverter() {
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

  @Override
  public int compareInt64(final long value, final Long referenceValue) {
    return Long.compareUnsigned(value, toInt64(referenceValue));
  }
}
