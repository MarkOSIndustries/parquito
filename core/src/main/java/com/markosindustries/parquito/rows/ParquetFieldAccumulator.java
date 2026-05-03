package com.markosindustries.parquito.rows;

public interface ParquetFieldAccumulator<Field> {
  /**
   * Accumulate a null and return the estimated extra bytes required
   *
   * @param repetitionLevel The repetition level of the null
   * @param definitionLevel The definition level of the null
   * @return The estimated bytes required
   */
  int accumulateNull(int repetitionLevel, int definitionLevel);

  /**
   * Accumulate a value and return the estimated extra bytes required
   *
   * @param repetitionLevel The repetition level of the value
   * @param value The value to accumulate
   * @return The estimated bytes required for that value
   */
  int accumulate(final int repetitionLevel, final Field value);

  /**
   * Accumulate a value and return the estimated extra bytes required
   *
   * @param repetitionLevel The repetition level of the value
   * @param fieldValue The value to accumulate
   * @return The estimated bytes required for that value
   */
  default int accumulateObject(final int repetitionLevel, final Object fieldValue) {
    //noinspection unchecked
    return accumulate(repetitionLevel, (Field) fieldValue);
  }
}
