package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.Writer;
import java.util.Objects;

public class LeafAccumulator<Leaf, WriteAs> {
  private final WriteTranslator<Leaf, WriteAs> writeTranslator;
  private final ParquetSchemaNode schemaNode;
  private final ColumnChunkWriter<WriteAs> columnChunkWriter;

  public LeafAccumulator(
      final WriteTranslator<Leaf, WriteAs> writeTranslator,
      final ParquetSchemaNode schemaNode,
      final Writer.DataPageAccumulator dataPageAccumulator) {
    this.writeTranslator = writeTranslator;
    this.schemaNode = schemaNode;
    //noinspection unchecked
    this.columnChunkWriter =
        (ColumnChunkWriter<WriteAs>) dataPageAccumulator.getColumnChunkWriter(schemaNode.getPath());
  }

  public void accumulateNull(final int repetitionLevel, final int definitionLevel) {
    columnChunkWriter.accumulateNull(repetitionLevel, definitionLevel);
  }

  protected void accumulateSingle(final int repetitionLevel, final Leaf value) {
    columnChunkWriter.accumulateValue(
        repetitionLevel, writeTranslator.translate(Objects.requireNonNull(value)));
  }

  public <RepeatedValues extends Iterable<Leaf>> void accumulateRepeated(
      final int repetitionLevel, final RepeatedValues values) {
    var rLevel = repetitionLevel;
    for (final var value : values) {
      if (value == null) {
        columnChunkWriter.accumulateNull(rLevel, schemaNode.getDefinitionLevelMax());
      } else {
        columnChunkWriter.accumulateValue(rLevel, writeTranslator.translate(value));
      }
      rLevel = schemaNode.getRepetitionLevelMax();
    }
    if (rLevel == repetitionLevel) {
      // We didn't write any values - we need a sentinel write
      columnChunkWriter.accumulateNull(repetitionLevel, schemaNode.getDefinitionLevelMax() - 1);
    }
  }

  public static class Optional<Leaf, WriteAs> extends LeafAccumulator<Leaf, WriteAs>
      implements ParquetFieldAccumulator<Leaf> {
    public Optional(
        final WriteTranslator<Leaf, WriteAs> writeTranslator,
        final ParquetSchemaNode schemaNode,
        final Writer.DataPageAccumulator dataPageAccumulator) {
      super(writeTranslator, schemaNode, dataPageAccumulator);
    }

    @Override
    public void accumulate(final int repetitionLevel, final Leaf value) {
      accumulateSingle(repetitionLevel, value);
    }
  }

  public static class Repeated<Leaf, RepeatedValues extends Iterable<Leaf>, WriteAs>
      extends LeafAccumulator<Leaf, WriteAs> implements ParquetFieldAccumulator<RepeatedValues> {
    public Repeated(
        final WriteTranslator<Leaf, WriteAs> writeTranslator,
        final ParquetSchemaNode schemaNode,
        final Writer.DataPageAccumulator dataPageAccumulator) {
      super(writeTranslator, schemaNode, dataPageAccumulator);
    }

    @Override
    public void accumulate(final int repetitionLevel, final RepeatedValues values) {
      accumulateRepeated(repetitionLevel, values);
    }
  }
}
