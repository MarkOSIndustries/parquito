package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowGroupWriter;
import java.nio.ByteBuffer;
import org.apache.parquet.format.Type;

public class LeafAccumulator implements ParquetFieldAccumulator {
  private final ParquetSchemaNode schemaNode;
  private final AccumulatorState state;
  private final ColumnChunkWriter columnChunkWriter;

  public int repetitionLevel = 0;

  LeafAccumulator(
      final ParquetSchemaNode schemaNode,
      final RowGroupWriter<?> rowGroupWriter,
      final AccumulatorState state) {
    this.schemaNode = schemaNode;
    this.state = state;
    //noinspection unchecked
    this.columnChunkWriter = rowGroupWriter.getColumnChunkWriter(schemaNode.getPath());
  }

  @Override
  public void beginBranch() {}

  @Override
  public void endBranch(final int newRepetitionLevel) {
    this.repetitionLevel = newRepetitionLevel;
  }

  public Type getType() {
    return schemaNode.getElement().type;
  }

  @Override
  public void accumulateNull() {
    columnChunkWriter.accumulateNull(this.repetitionLevel, schemaNode.getDefinitionLevelMax() - 1);
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  @Override
  public void accumulateNull(int repetitionLevel, int definitionLevel) {
    columnChunkWriter.accumulateNull(repetitionLevel, definitionLevel);
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateBoolean(final boolean value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateByteBuffer(final ByteBuffer value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateDouble(final double value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateFloat(final float value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateInt32(final int value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }

  public void accumulateInt64(final long value) {
    state.incrementEstimatedBytesRequired(
        columnChunkWriter.accumulateValue(this.repetitionLevel, value));
    this.repetitionLevel = schemaNode.getRepetitionLevelMax();
  }
}
