package com.markosindustries.parquito.types;

import java.time.Instant;
import org.apache.parquet.format.Type;

public class InstantMicrosConverter extends AbstractLogicalTypeConverter<Instant> {
  public static final InstantMicrosConverter INSTANCE = new InstantMicrosConverter();

  public InstantMicrosConverter() {
    super(Instant.class);
  }

  @Override
  public Type getType() {
    return Type.INT64;
  }

  @Override
  public Instant fromInt64(final long value) {
    final var epochSeconds = Math.floorDiv(value, 1_000_000L);
    return Instant.ofEpochSecond(epochSeconds, value - (epochSeconds * 1_000_000L));
  }

  @Override
  public long toInt64(final Instant value) {
    return (value.getEpochSecond() * 1_000_000L) + Math.floorDiv(value.getNano(), 1_000L);
  }
}
