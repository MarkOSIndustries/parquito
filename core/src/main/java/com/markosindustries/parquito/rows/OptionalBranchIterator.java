package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;
import java.util.Iterator;
import java.util.Map;

public class OptionalBranchIterator<Branch> implements ParquetFieldIterator<Branch> {
  private final Map<String, ParquetFieldIterator<?>> childIterators;
  private final ParquetSchemaNode schemaNode;
  private final RowReadSpec<?, Branch, ?> rowReadSpec;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;

  public OptionalBranchIterator(
      final Map<String, ParquetFieldIterator<?>> childIterators,
      final ParquetSchemaNode schemaNode,
      final RowReadSpec<?, Branch, ?> rowReadSpec) {
    this.childIterators = childIterators;
    this.schemaNode = schemaNode;
    this.hasNext = childIterators.values().stream().anyMatch(Iterator::hasNext);
    this.definitionLevel =
        hasNext
            ? childIterators.values().stream()
                .mapToInt(ParquetFieldIterator::peekDefinitionLevel)
                .max()
                .orElseThrow()
            : 0;
    this.repetitionLevel =
        hasNext
            ? childIterators.values().stream()
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
    for (final var iterator : childIterators.values()) {
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
    for (final var entry : childIterators.entrySet()) {
      final var child = entry.getKey();
      final var iterator = entry.getValue();
      final var next = iterator.next();
      if (!isNull) {
        result.put(child, next);
      }
      if (!iterator.hasNext()) {
        hasNext = false;
      } else {
        definitionLevel = Math.max(definitionLevel, iterator.peekDefinitionLevel());
        repetitionLevel = Math.max(repetitionLevel, iterator.peekRepetitionLevel());
      }
    }
    return result == null ? null : result.build();
  }
}
