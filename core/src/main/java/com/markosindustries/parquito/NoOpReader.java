package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;

public class NoOpReader implements Reader<Object, Object> {
  public static final NoOpReader INSTANCE = new NoOpReader();

  private NoOpReader() {}

  @Override
  public Reader<?, ?> forChild(final int childFieldIndex) {
    return this;
  }

  @Override
  public BranchBuilder<Object> branchBuilder() {
    return new NullBranchBuilder();
  }

  @Override
  public RepeatedBuilder<Object, Object> repeatedBuilder() {
    return new NullRepeatedBuilder();
  }

  private static class NullBranchBuilder implements BranchBuilder<Object> {
    @Override
    public void put(final int fieldIndex, final Object value) {}

    @Override
    public Object build() {
      return null;
    }
  }

  private static class NullRepeatedBuilder implements RepeatedBuilder<Object, Object> {
    @Override
    public void add(final Object unused) {}

    @Override
    public Object build() {
      return null;
    }
  }
}
