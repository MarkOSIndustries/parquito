package com.markosindustries.parquito.types;

import java.time.Instant;
import org.apache.parquet.format.Type;

public class InstantMillisConverter extends AbstractLogicalTypeConverter<Instant> {
  public static final InstantMillisConverter INSTANCE = new InstantMillisConverter();

  public InstantMillisConverter() {
    super(Instant.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Instant fromInt64(final long value) {
    return Instant.ofEpochMilli(value);
  }

  @Override
  public long toInt64(final Instant value) {
    return value.toEpochMilli();
  }
}
