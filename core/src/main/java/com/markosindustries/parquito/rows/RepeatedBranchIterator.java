package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;

public class RepeatedBranchIterator<Repeated, Value> implements ParquetFieldIterator<Repeated> {
  private final OptionalBranchIterator<Value> optionalBranchIterator;
  private final ParquetSchemaNode schemaNode;
  private final Reader<Repeated, Value> reader;

  public RepeatedBranchIterator(
      ParquetFieldIterator<?>[] childIterators,
      ParquetSchemaNode schemaNode,
      final Reader<Repeated, Value> reader) {
    this.reader = reader;
    this.optionalBranchIterator = new OptionalBranchIterator<>(childIterators, schemaNode, reader);
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
  public void skipNextRow() {
    optionalBranchIterator.skipNextRow();
  }

  @Override
  public Repeated next() {
    final var values = reader.repeatedBuilder();
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
