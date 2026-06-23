package com.markosindustries.parquito.types;

import java.time.Duration;
import java.time.Instant;
import org.apache.parquet.format.Type;

public class InstantDatesConverter extends AbstractLogicalTypeConverter<Instant> {
  public static final InstantDatesConverter INSTANCE = new InstantDatesConverter();

  public InstantDatesConverter() {
    super(Instant.class);
  }

  @Override
  public Type getType() {
    return Type.INT32;
  }

  @Override
  public Instant fromInt32(final int value) {
    return Instant.ofEpochSecond(Duration.ofDays(value).getSeconds());
  }

  @Override
  public int toInt32(final Instant value) {
    return (int) Duration.ofSeconds(value.getEpochSecond()).toDays();
  }
}
