package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.Writer;

public class RowAccumulator<Row> {
  private final ParquetFieldAccumulator<Row> accumulator;

  public RowAccumulator(
      final ParquetSchemaNode.Root schemaRoot,
      final WriteTranslator<Row, ?> translator,
      final Writer.DataPageAccumulator dataPageAccumulator) {
    accumulator = new BranchAccumulator.Optional<>(translator, schemaRoot, dataPageAccumulator);
  }

  public void accumulate(final Row row) {
    accumulator.accumulate(0, row);
  }
}
