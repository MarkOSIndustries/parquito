package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowGroupWriter;

import java.util.Arrays;
import java.util.function.Consumer;

public class BranchAccumulator implements ParquetFieldAccumulator {
  private final ParquetSchemaNode schemaNode;
  private final AccumulatorState state;
  private final ParquetFieldAccumulator[] fieldAccumulatorsByChildIndex;
  private final boolean[] needsNullByChildIndex;

  private int repetitionLevel = 0;

  BranchAccumulator(
      final ParquetSchemaNode schemaNode,
      final RowGroupWriter<?> rowGroupWriter,
      final AccumulatorState state) {
    this.schemaNode = schemaNode;
    this.state = state;
    this.fieldAccumulatorsByChildIndex =
        new ParquetFieldAccumulator[schemaNode.getChildren().length];
    for (var childIndex = 0; childIndex < schemaNode.getChildren().length; childIndex++) {
      final var childSchemaNode = schemaNode.getChildAtIndex(childIndex);
      if (childSchemaNode.getChildren().length == 0) {
        fieldAccumulatorsByChildIndex[childIndex] =
            new LeafAccumulator(childSchemaNode, rowGroupWriter, state);
      } else {
        fieldAccumulatorsByChildIndex[childIndex] =
            new BranchAccumulator(childSchemaNode, rowGroupWriter, state);
      }
    }
    this.needsNullByChildIndex = new boolean[schemaNode.getChildren().length];
  }

  @Override
  public void beginBranch() {
    Arrays.fill(needsNullByChildIndex, true);
  }

  @Override
  public void endBranch(final int newRepetitionLevel) {
    for (var i = 0; i < needsNullByChildIndex.length; i++) {
      if (needsNullByChildIndex[i]) {
        fieldAccumulatorsByChildIndex[i].accumulateNull(
            repetitionLevel, this.schemaNode.getDefinitionLevelMax());
      }
    }

    for (final var fieldAccumulator : fieldAccumulatorsByChildIndex) {
      fieldAccumulator.endBranch(newRepetitionLevel);
    }

    this.repetitionLevel = newRepetitionLevel;
  }

  public interface ChildAccessor {
    BranchAccumulator childBranchAccumulator(int childIndex);

    LeafAccumulator childLeafAccumulator(int childIndex);
  }

  private final ChildAccessor childAccessor =
      new ChildAccessor() {
        @Override
        public BranchAccumulator childBranchAccumulator(final int childIndex) {
          needsNullByChildIndex[childIndex] = false;
          return (BranchAccumulator) fieldAccumulatorsByChildIndex[childIndex];
        }

        @Override
        public LeafAccumulator childLeafAccumulator(final int childIndex) {
          needsNullByChildIndex[childIndex] = false;
          return (LeafAccumulator) fieldAccumulatorsByChildIndex[childIndex];
        }
      };

  public void branch(Consumer<ChildAccessor> translateBranch) {
    beginBranch();
    translateBranch.accept(childAccessor);
    endBranch(this.schemaNode.getRepetitionLevelMax());
  }

  @Override
  public void accumulateNull() {
    accumulateNull(repetitionLevel, this.schemaNode.getDefinitionLevelMax() - 1);
    repetitionLevel = this.schemaNode.getRepetitionLevelMax();
  }

  @Override
  public void accumulateNull(int repetitionLevel, int definitionLevel) {
    for (int childIndex = 0; childIndex < fieldAccumulatorsByChildIndex.length; childIndex++) {
      needsNullByChildIndex[childIndex] = false;
      final var fieldAccumulator = fieldAccumulatorsByChildIndex[childIndex];
      fieldAccumulator.accumulateNull(repetitionLevel, definitionLevel);
    }
  }
}
