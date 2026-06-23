package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class BooleanConverter extends AbstractLogicalTypeConverter<Boolean> {
  public static final BooleanConverter INSTANCE = new BooleanConverter();

  public BooleanConverter() {
    super(Boolean.class);
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN;
  }

  @Override
  public Boolean fromBoolean(final boolean value) {
    return value;
  }

  @Override
  public boolean toBoolean(final Boolean value) {
    return value;
  }
}
