package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import java.util.Iterator;
import java.util.Map;

public class OptionalBranchIterator<Branch> implements ParquetFieldIterator<Branch> {
  private final Map<String, ParquetFieldIterator<?>> childIterators;
  private final ParquetSchemaNode schemaNode;
  private boolean hasNext;
  private int definitionLevel;
  private int repetitionLevel;
  private final Reader<?, Branch> reader;

  public OptionalBranchIterator(
      final Map<String, ParquetFieldIterator<?>> childIterators,
      final ParquetSchemaNode schemaNode,
      final Reader<?, Branch> reader) {
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
    this.reader = reader;
  }
  //
  //  private Branch materialiseNext() {
  //    // Always have next ready to go by doing the work in constructor()/next(). Allows predicate
  // to align with hasNext
  //
  //    // Do branches which have predicates first
  //    // Apply predicate to half-baked result
  //    // "Finish it off" with non-predicate children
  //  }
  //
  //  /**
  //   * Skip rows until we find one where the given predicate is true
  //   * @param parquetPredicate
  //   */
  //  public void skipUntil(ParquetPredicate parquetPredicate) {
  //
  //  }
  //
  //  private boolean peekPredicateFailure() {
  //    if(!hasPredicate) {
  //      return false;
  //    }
  //    for (final var entry : childIterators.entrySet()) {
  //      final var child = entry.getKey();
  //      final var iterator = entry.getValue();
  //      if(iterator.peekPredicateFailure()) {
  //        return true;
  //      }
  //    }
  //    return false;
  //  }
  //
  //  private void skipPredicateFailures() {
  //    if(hasNext) {
  //        boolean anyPredicateFailure;
  //        do {
  //          anyPredicateFailure = false;
  //          for (final var entry : childIterators.entrySet()) {
  //            final var child = entry.getKey();
  //            final var iterator = entry.getValue();
  //            if(iterator.peekPredicateFailure()) {
  //              anyPredicateFailure = true;
  //              break;
  //            }
  //          }
  //        } while (anyPredicateFailure && hasNext);
  //      }
  //  }

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
  public Branch next() {
    boolean isNull = definitionLevel < schemaNode.getDefinitionLevelMax();
    final var result = isNull ? null : reader.branchBuilder();
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
