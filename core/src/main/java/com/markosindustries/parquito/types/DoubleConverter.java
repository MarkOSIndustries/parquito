package com.markosindustries.parquito.types;

import org.apache.parquet.format.Type;

public class DoubleConverter extends AbstractLogicalTypeConverter<Double> {
  public static final DoubleConverter INSTANCE = new DoubleConverter();

  public DoubleConverter() {
    super(Double.class);
  }

  @Override
  public Type getType() {
    return Type.DOUBLE;
  }

  @Override
  public Double fromDouble(final double value) {
    return value;
  }

  @Override
  public double toDouble(final Double value) {
    return value;
  }
}
