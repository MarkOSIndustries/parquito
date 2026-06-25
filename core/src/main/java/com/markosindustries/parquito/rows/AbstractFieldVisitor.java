package com.markosindustries.parquito.rows;

public abstract class AbstractFieldVisitor implements FieldVisitor {
  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    throw new UnsupportedOperationException("Unexpected recursion in field visitor");
  }

  @Override
  public void endBranch() {
    throw new UnsupportedOperationException("Unexpected end of branch");
  }

  @Override
  public void endRepeated() {
    throw new UnsupportedOperationException("Unexpected end of repeated");
  }

  @Override
  public void visitNull(final int pageIndex) {
    throw new UnsupportedOperationException("Unexpected null value");
  }
}
