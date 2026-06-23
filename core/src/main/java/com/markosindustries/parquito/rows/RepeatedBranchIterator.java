package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;

public class RepeatedBranchIterator implements ParquetFieldIterator {
  private final OptionalBranchIterator optionalBranchIterator;
  private final ParquetSchemaNode schemaNode;

  public RepeatedBranchIterator(
      ParquetFieldIterator[] childIterators, ParquetSchemaNode schemaNode) {
    this.optionalBranchIterator = new OptionalBranchIterator(childIterators, schemaNode);
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
  public void visitNext(final FieldVisitor visitor) {
    if (optionalBranchIterator.peekDefinitionLevel() >= schemaNode.getDefinitionLevelMax()) {
      do {
        optionalBranchIterator.visitNext(visitor);
      } while (optionalBranchIterator.peekRepetitionLevel() >= schemaNode.getRepetitionLevelMax());
    } else {
      optionalBranchIterator.visitNext(NoOpFieldVisitor.INSTANCE);
    }
    visitor.endRepeated();
  }
}
