package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;

public class OptionalBranchIterator<Branch> implements ParquetFieldIterator<Branch> {
  private final ParquetFieldIterator<?>[] childIterators;
  private final ParquetSchemaNode schemaNode;
  private final Reader<?, Branch> reader;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;

  public OptionalBranchIterator(
      final ParquetFieldIterator<?>[] childIterators,
      final ParquetSchemaNode schemaNode,
      final Reader<?, Branch> reader) {
    this.childIterators = childIterators;
    this.schemaNode = schemaNode;
    this.reader = reader;
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
  public Branch next() {
    boolean isNull = definitionLevel < schemaNode.getDefinitionLevelMax();
    final var result = isNull ? null : reader.branchBuilder();
    definitionLevel = 0;
    repetitionLevel = 0;
    for (var childIndex = 0; childIndex < childIterators.length; childIndex++) {
      final var iterator = childIterators[childIndex];
      final var next = iterator.next();
      if (!isNull) {
        result.put(childIndex, next);
      }
      if (iterator.hasNext()) {
        definitionLevel = Math.max(definitionLevel, iterator.peekDefinitionLevel());
        repetitionLevel = Math.max(repetitionLevel, iterator.peekRepetitionLevel());
      } else {
        hasNext = false;
      }
    }
    return result == null ? null : result.build();
  }
}
