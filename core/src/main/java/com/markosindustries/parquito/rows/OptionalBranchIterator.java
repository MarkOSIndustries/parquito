package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;

public class OptionalBranchIterator implements ParquetFieldIterator {
  private final ParquetFieldIterator[] childIterators;
  private final ParquetSchemaNode schemaNode;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;

  public OptionalBranchIterator(
      final ParquetFieldIterator[] childIterators, final ParquetSchemaNode schemaNode) {
    this.childIterators = childIterators;
    this.schemaNode = schemaNode;
    this.hasNext = false;
    this.definitionLevel = 0;
    this.repetitionLevel = 0;
    for (final var childIterator : childIterators) {
      if (childIterator == null) {
        continue;
      }
      hasNext = hasNext || childIterator.hasNext();
      definitionLevel = Math.max(definitionLevel, childIterator.peekDefinitionLevel());
      repetitionLevel = Math.max(repetitionLevel, childIterator.peekRepetitionLevel());
    }
  }

  @Override
  public int peekDefinitionLevel() {
    return definitionLevel;
  }

  @Override
  public int peekRepetitionLevel() {
    return repetitionLevel;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public void skipNextRow() {
    for (final var childIterator : childIterators) {
      childIterator.skipNextRow();
      if (!childIterator.hasNext()) {
        hasNext = false;
      }
    }
  }

  @Override
  public void visitNext(final FieldVisitor visitor) {
    boolean isNull = definitionLevel < schemaNode.getDefinitionLevelMax();
    definitionLevel = 0;
    repetitionLevel = 0;
    for (var childIndex = 0; childIndex < childIterators.length; childIndex++) {
      final var iterator = childIterators[childIndex];

      if (!isNull) {
        iterator.visitNext(visitor.forChildIndex(childIndex));
      } else {
        iterator.visitNext(NoOpFieldVisitor.INSTANCE);
      }

      if (iterator.hasNext()) {
        definitionLevel = Math.max(definitionLevel, iterator.peekDefinitionLevel());
        repetitionLevel = Math.max(repetitionLevel, iterator.peekRepetitionLevel());
      } else {
        hasNext = false;
      }
    }
    if (!isNull) {
      visitor.endBranch();
    }
  }
}
