package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class FloatConverter extends AbstractLogicalTypeConverter<Float> {
  public static final FloatConverter INSTANCE = new FloatConverter();

  public FloatConverter() {
    super(Float.class);
  }

  @Override
  public Type getType() {
    return Type.FLOAT;
  }

  @Override
  public Float fromFloat(final float value) {
    return value;
  }

  @Override
  public float toFloat(final Float value) {
    return value;
  }
}
