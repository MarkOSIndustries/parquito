package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;
import com.markosindustries.parquito.SparseArrayIndexMap;
import java.util.Iterator;

public class OptionalBranchIterator<Branch> implements ParquetFieldIterator<Branch> {
  private final SparseArrayIndexMap<ParquetFieldIterator<?>> childIterators;
  private final ParquetSchemaNode schemaNode;
  private final RowReadSpec<?, Branch, ?> rowReadSpec;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;

  public OptionalBranchIterator(
      final SparseArrayIndexMap<ParquetFieldIterator<?>> childIterators,
      final ParquetSchemaNode schemaNode,
      final RowReadSpec<?, Branch, ?> rowReadSpec) {
    this.childIterators = childIterators;
    this.schemaNode = schemaNode;
    this.hasNext = childIterators.valuesStream().anyMatch(Iterator::hasNext);
    this.definitionLevel =
        hasNext
            ? childIterators
                .valuesStream()
                .mapToInt(ParquetFieldIterator::peekDefinitionLevel)
                .max()
                .orElseThrow()
            : 0;
    this.repetitionLevel =
        hasNext
            ? childIterators
                .valuesStream()
                .mapToInt(ParquetFieldIterator::peekRepetitionLevel)
                .max()
                .orElseThrow()
            : 0;
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
    return rowReadSpec
        .predicate()
        .branchMatches(child -> childIterators.get(child).nextRowMatches());
  }

  @Override
  public void skipNextRow() {
    for (final var childFieldId : childIterators.indexes()) {
      final var iterator = childIterators.get(childFieldId);
      iterator.skipNextRow();
      if (!iterator.hasNext()) {
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
    for (final var childFieldId : childIterators.indexes()) {
      final var iterator = childIterators.get(childFieldId);
      final var next = iterator.next();
      if (!isNull) {
        result.put(childFieldId, next);
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
