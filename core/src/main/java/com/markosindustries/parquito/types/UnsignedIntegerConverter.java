package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class UnsignedIntegerConverter extends AbstractLogicalTypeConverter<Integer> {
  public static final UnsignedIntegerConverter INSTANCE = new UnsignedIntegerConverter();

  public UnsignedIntegerConverter() {
    super(Integer.class);
  }

  @Override
  public Type getType() {
    return Type.INT32;
  }

  @Override
  public Integer fromInt32(final int value) {
    return value;
  }

  @Override
  public int toInt32(final Integer value) {
    return value;
  }

  @Override
  public int compareInt32(final int value, final Integer referenceValue) {
    return Integer.compareUnsigned(value, toInt32(referenceValue));
  }
}
