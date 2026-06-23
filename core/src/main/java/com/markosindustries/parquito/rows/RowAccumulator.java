package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowGroupWriter;
import com.markosindustries.parquito.WriteTranslator;

public class RowAccumulator<Row> {
  private final WriteTranslator<Row> writeTranslator;
  private final AccumulatorState accumulatorState;
  private final BranchAccumulator accumulator;

  public RowAccumulator(
      final ParquetSchemaNode.Root schemaRoot,
      final WriteTranslator<Row> translator,
      final RowGroupWriter<?> rowGroupWriter) {
    this.writeTranslator = translator;
    this.accumulatorState = new AccumulatorState();
    this.accumulator = new BranchAccumulator(schemaRoot, rowGroupWriter, accumulatorState);
  }

  /**
   * Accumulate a row and return the estimated extra bytes required
   *
   * @param row The row to accumulate
   */
  public void accumulate(final Row row) {
    writeTranslator.translate(row, accumulator);
  }

  public int estimatedBytesRequired() {
    return accumulatorState.estimatedBytesRequired();
  }

  public void resetEstimatedBytesRequired() {
    accumulatorState.resetEstimatedBytesRequired();
  }
}
