package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;
import java.util.Map;

public class RepeatedBranchIterator<Repeated, Value> implements ParquetFieldIterator<Repeated> {
  private final OptionalBranchIterator<Value> optionalBranchIterator;
  private final ParquetSchemaNode schemaNode;
  private final RowReadSpec<Repeated, Value, ?> rowReadSpec;

  public RepeatedBranchIterator(
      Map<String, ParquetFieldIterator<?>> childIterators,
      ParquetSchemaNode schemaNode,
      RowReadSpec<Repeated, Value, ?> rowReadSpec) {
    this.rowReadSpec = rowReadSpec;
    this.optionalBranchIterator =
        new OptionalBranchIterator<>(childIterators, schemaNode, rowReadSpec);
    this.schemaNode = schemaNode;
  }

  @Override
  public int peekDefinitionLevel() {
    return optionalBranchIterator.peekDefinitionLevel();
  }

  @Override
  public int peekRepetitionLevel() {
    return optionalBranchIterator.peekRepetitionLevel();
  }

  @Override
  public boolean hasNext() {
    return optionalBranchIterator.hasNext();
  }

  @Override
  public boolean nextRowMatches() {
    return optionalBranchIterator.nextRowMatches();
  }

  @Override
  public void skipNextRow() {
    optionalBranchIterator.skipNextRow();
  }

  @Override
  public Repeated next() {
    final var values = rowReadSpec.reader().repeatedBuilder();
    if (optionalBranchIterator.peekDefinitionLevel() >= schemaNode.getDefinitionLevelMax()) {
      do {
        values.add(optionalBranchIterator.next());
      } while (optionalBranchIterator.peekRepetitionLevel() >= schemaNode.getRepetitionLevelMax());
    } else {
      optionalBranchIterator.next();
    }
    return values.build();
  }
}
