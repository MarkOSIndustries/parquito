package com.markosindustries.parquito.types;

import java.time.Duration;
import org.apache.parquet.format.Type;

public class DurationMicrosConverter extends AbstractLogicalTypeConverter<Duration> {
  public static final DurationMicrosConverter INSTANCE = new DurationMicrosConverter();

  public DurationMicrosConverter() {
    super(Duration.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Duration fromInt64(final long value) {
    return Duration.ofNanos(value * 1_000);
  }

  @Override
  public long toInt64(final Duration value) {
    return (value.getSeconds() * 1_000_000L) + (Math.floorDiv(value.getNano(), 1_000L));
  }
}
