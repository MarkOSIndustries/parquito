package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;

public class RowAccumulator<Row> {
  private final ParquetFieldAccumulator<Row> accumulator;

  public RowAccumulator(
      final ParquetSchemaNode.Root schemaRoot,
      final WriteTranslator<Row, ?> translator,
      final ValueAccumulator valueAccumulator) {
    accumulator = new BranchAccumulator.Optional<>(translator, schemaRoot, valueAccumulator);
  }

  /**
   * Accumulate a row and return the estimated extra bytes required
   *
   * @param row The row to accumulate
   * @return The estimated bytes required for that row
   */
  public int accumulate(final Row row) {
    return accumulator.accumulate(0, row);
  }
}
