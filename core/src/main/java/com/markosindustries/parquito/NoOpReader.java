package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.NoOpFieldVisitor;

public class NoOpReader implements Reader<Object> {
  public static final NoOpReader INSTANCE = new NoOpReader();

  private NoOpReader() {}

  static class NoOpRowBuilder extends NoOpFieldVisitor implements RowBuilder<Object> {
    @Override
    public Object build() {
      return null;
    }
  }

  @Override
  public RowBuilder<Object> rowBuilder() {
    return new NoOpRowBuilder();
  }
}
