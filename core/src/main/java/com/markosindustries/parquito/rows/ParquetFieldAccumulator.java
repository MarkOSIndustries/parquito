package com.markosindustries.parquito.rows;

public interface ParquetFieldAccumulator<Field> {
  void accumulateNull(int repetitionLevel, int definitionLevel);

  void accumulate(final int repetitionLevel, final Field value);

  default void accumulateObject(final int repetitionLevel, final Object fieldValue) {
    //noinspection unchecked
    accumulate(repetitionLevel, (Field) fieldValue);
  }
}
