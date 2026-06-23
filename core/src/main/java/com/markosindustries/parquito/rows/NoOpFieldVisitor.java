package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.page.Values;

public class NoOpFieldVisitor extends Values.NoOpVisitor implements FieldVisitor {
  public static final NoOpFieldVisitor INSTANCE = new NoOpFieldVisitor();

  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    return this;
  }

  @Override
  public void endBranch() {}

  @Override
  public void endRepeated() {}
}
