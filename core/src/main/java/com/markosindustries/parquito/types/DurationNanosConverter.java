package com.markosindustries.parquito.types;

import java.time.Duration;
import org.apache.parquet.format.Type;

public class DurationNanosConverter extends AbstractLogicalTypeConverter<Duration> {
  public static final DurationNanosConverter INSTANCE = new DurationNanosConverter();

  public DurationNanosConverter() {
    super(Duration.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Duration fromInt64(final long value) {
    return Duration.ofNanos(value);
  }

  @Override
  public long toInt64(final Duration value) {
    return value.toNanos();
  }
}
