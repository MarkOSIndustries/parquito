package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import java.util.Objects;

public class LeafAccumulator<Leaf, WriteAs> {
  private final WriteTranslator<Leaf, WriteAs> writeTranslator;
  private final ParquetSchemaNode schemaNode;
  private final ColumnChunkWriter<WriteAs> columnChunkWriter;

  public LeafAccumulator(
      final WriteTranslator<Leaf, WriteAs> writeTranslator,
      final ParquetSchemaNode schemaNode,
      final ValueAccumulator valueAccumulator) {
    this.writeTranslator = writeTranslator;
    this.schemaNode = schemaNode;
    //noinspection unchecked
    this.columnChunkWriter =
        (ColumnChunkWriter<WriteAs>) valueAccumulator.getColumnChunkWriter(schemaNode.getPath());
  }

  public int accumulateNull(final int repetitionLevel, final int definitionLevel) {
    return columnChunkWriter.accumulateNull(repetitionLevel, definitionLevel);
  }

  protected int accumulateSingle(final int repetitionLevel, final Leaf value) {
    return columnChunkWriter.accumulateValue(
        repetitionLevel, writeTranslator.translate(Objects.requireNonNull(value)));
  }

  public <RepeatedValues extends Iterable<Leaf>> int accumulateRepeated(
      final int repetitionLevel, final RepeatedValues values) {
    var rLevel = repetitionLevel;
    var bytes = 0;
    for (final var value : values) {
      if (value == null) {
        bytes += columnChunkWriter.accumulateNull(rLevel, schemaNode.getDefinitionLevelMax());
      } else {
        bytes += columnChunkWriter.accumulateValue(rLevel, writeTranslator.translate(value));
      }
      rLevel = schemaNode.getRepetitionLevelMax();
    }
    if (rLevel == repetitionLevel) {
      // We didn't write any values - we need a sentinel write
      bytes +=
          columnChunkWriter.accumulateNull(repetitionLevel, schemaNode.getDefinitionLevelMax() - 1);
    }
    return bytes;
  }

  public static class Optional<Leaf, WriteAs> extends LeafAccumulator<Leaf, WriteAs>
      implements ParquetFieldAccumulator<Leaf> {
    public Optional(
        final WriteTranslator<Leaf, WriteAs> writeTranslator,
        final ParquetSchemaNode schemaNode,
        final ValueAccumulator valueAccumulator) {
      super(writeTranslator, schemaNode, valueAccumulator);
    }

    @Override
    public int accumulate(final int repetitionLevel, final Leaf value) {
      return accumulateSingle(repetitionLevel, value);
    }
  }

  public static class Repeated<Leaf, RepeatedValues extends Iterable<Leaf>, WriteAs>
      extends LeafAccumulator<Leaf, WriteAs> implements ParquetFieldAccumulator<RepeatedValues> {
    public Repeated(
        final WriteTranslator<Leaf, WriteAs> writeTranslator,
        final ParquetSchemaNode schemaNode,
        final ValueAccumulator valueAccumulator) {
      super(writeTranslator, schemaNode, valueAccumulator);
    }

    @Override
    public int accumulate(final int repetitionLevel, final RepeatedValues values) {
      return accumulateRepeated(repetitionLevel, values);
    }
  }
}
