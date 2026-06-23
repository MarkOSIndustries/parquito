package com.markosindustries.parquito.encoding;

import java.util.Map;
import org.apache.parquet.format.Type;

public final class FixedTypeLengths {
  public static final Map<Type, Integer> BYTES_BY_TYPE =
      Map.of(
          Type.BOOLEAN, 4,
          Type.INT32, 4,
          Type.INT64, 8,
          Type.FLOAT, 4,
          Type.DOUBLE, 8);

  private FixedTypeLengths() {}
}
