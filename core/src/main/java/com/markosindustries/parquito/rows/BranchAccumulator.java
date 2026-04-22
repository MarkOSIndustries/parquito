package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.Writer;

public abstract class BranchAccumulator<Branch, WriteAs> {
  private final WriteTranslator<Branch, WriteAs> writeTranslator;
  private final ParquetSchemaNode schemaNode;
  private final ParquetFieldAccumulator<?>[] fieldAccumulatorsByChildIndex;

  public BranchAccumulator(
      final WriteTranslator<Branch, WriteAs> writeTranslator,
      final ParquetSchemaNode schemaNode,
      final Writer.DataPageAccumulator dataPageAccumulator) {
    this.writeTranslator = writeTranslator;
    this.schemaNode = schemaNode;
    this.fieldAccumulatorsByChildIndex =
        new ParquetFieldAccumulator<?>[schemaNode.getChildren().length];
    for (var childIndex = 0; childIndex < schemaNode.getChildren().length; childIndex++) {
      final var childSchemaNode = schemaNode.getChildAtIndex(childIndex);
      final var childWriteTranslator = writeTranslator.forChildIndex(childIndex);
      if (childSchemaNode.getChildren().length == 0) {
        fieldAccumulatorsByChildIndex[childIndex] =
            switch (childSchemaNode.getRepetitionType()) {
              case REQUIRED, OPTIONAL ->
                  new LeafAccumulator.Optional<>(
                      childWriteTranslator, childSchemaNode, dataPageAccumulator);
              case REPEATED ->
                  new LeafAccumulator.Repeated<>(
                      childWriteTranslator, childSchemaNode, dataPageAccumulator);
            };
      } else {
        fieldAccumulatorsByChildIndex[childIndex] =
            switch (childSchemaNode.getRepetitionType()) {
              case REQUIRED, OPTIONAL ->
                  new BranchAccumulator.Optional<>(
                      childWriteTranslator, childSchemaNode, dataPageAccumulator);
              case REPEATED ->
                  new BranchAccumulator.Repeated<>(
                      childWriteTranslator, childSchemaNode, dataPageAccumulator);
            };
      }
    }
  }

  public void accumulateNull(final int repetitionLevel, final int definitionLevel) {
    for (final var fieldAccumulator : fieldAccumulatorsByChildIndex) {
      fieldAccumulator.accumulateNull(repetitionLevel, definitionLevel);
    }
  }

  protected <RepeatedValues extends Iterable<Branch>> void accumulateRepeated(
      final int repetitionLevel, final RepeatedValues values) {
    var rLevel = repetitionLevel;
    for (final var value : values) {
      if (value == null) {
        for (var childIndex = 0; childIndex < schemaNode.getChildren().length; childIndex++) {
          fieldAccumulatorsByChildIndex[childIndex].accumulateNull(
              rLevel, schemaNode.getDefinitionLevelMax());
        }
      } else {
        for (var childIndex = 0; childIndex < schemaNode.getChildren().length; childIndex++) {
          fieldAccumulatorsByChildIndex[childIndex].accumulateObject(rLevel, value);
        }
      }
      rLevel = schemaNode.getRepetitionLevelMax();
    }
    if (rLevel == repetitionLevel) {
      // We didn't write any values - we need a sentinel write
      accumulateNull(repetitionLevel, schemaNode.getDefinitionLevelMax() - 1);
    }
  }

  protected void accumulateSingle(final int repetitionLevel, final Branch value) {
    for (var childIndex = 0; childIndex < schemaNode.getChildren().length; childIndex++) {
      final var fieldValue = writeTranslator.getField(childIndex, value);
      if (fieldValue == null) {
        fieldAccumulatorsByChildIndex[childIndex].accumulateNull(
            repetitionLevel, schemaNode.getDefinitionLevelMax());
      } else {
        fieldAccumulatorsByChildIndex[childIndex].accumulateObject(repetitionLevel, fieldValue);
      }
    }
  }

  public static class Optional<Branch, WriteAs> extends BranchAccumulator<Branch, WriteAs>
      implements ParquetFieldAccumulator<Branch> {
    public Optional(
        final WriteTranslator<Branch, WriteAs> writeTranslator,
        final ParquetSchemaNode schemaNode,
        final Writer.DataPageAccumulator dataPageAccumulator) {
      super(writeTranslator, schemaNode, dataPageAccumulator);
    }

    @Override
    public void accumulate(final int repetitionLevel, final Branch value) {
      accumulateSingle(repetitionLevel, value);
    }
  }

  public static class Repeated<Branch, RepeatedValues extends Iterable<Branch>, WriteAs>
      extends BranchAccumulator<Branch, WriteAs>
      implements ParquetFieldAccumulator<RepeatedValues> {
    public Repeated(
        final WriteTranslator<Branch, WriteAs> writeTranslator,
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
