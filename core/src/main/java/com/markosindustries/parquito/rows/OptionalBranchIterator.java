package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;

public class OptionalBranchIterator<Branch> implements ParquetFieldIterator<Branch> {
  private final ParquetFieldIterator<?>[] childIterators;
  private final ParquetSchemaNode schemaNode;
  private final RowReadSpec<?, Branch, ?> rowReadSpec;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;

  public OptionalBranchIterator(
      final ParquetFieldIterator<?>[] childIterators,
      final ParquetSchemaNode schemaNode,
      final RowReadSpec<?, Branch, ?> rowReadSpec) {
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
    this.rowReadSpec = rowReadSpec;
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
  public boolean nextRowMatches() {
    return rowReadSpec.predicate().branchMatches(child -> childIterators[child].nextRowMatches());
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
    final var result = isNull ? null : rowReadSpec.reader().branchBuilder();
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
