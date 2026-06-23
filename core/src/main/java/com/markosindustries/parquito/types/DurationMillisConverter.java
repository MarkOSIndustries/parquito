package com.markosindustries.parquito.types;

import java.time.Duration;
import org.apache.parquet.format.Type;

public class DurationMillisConverter extends AbstractLogicalTypeConverter<Duration> {
  public static final DurationMillisConverter INSTANCE = new DurationMillisConverter();

  public DurationMillisConverter() {
    super(Duration.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Duration fromInt64(final long value) {
    return Duration.ofMillis(value);
  }

  @Override
  public long toInt64(final Duration value) {
    return value.toMillis();
  }
}
