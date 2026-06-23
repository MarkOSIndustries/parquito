package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class IntegerConverter extends AbstractLogicalTypeConverter<Integer> {
  public static final IntegerConverter INSTANCE = new IntegerConverter();

  public IntegerConverter() {
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
}
