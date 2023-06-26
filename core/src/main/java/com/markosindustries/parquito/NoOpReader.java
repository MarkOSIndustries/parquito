package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;

public class NoOpReader implements Reader<Void, Void> {
  public static final NoOpReader INSTANCE = new NoOpReader();

  private NoOpReader() {}

  @Override
  public Reader<?, ?> getChild(final String child) {
    return this;
  }

  @Override
  public BranchBuilder<Void> branchBuilder() {
    return new NullBranchBuilder();
  }

  @Override
  public RepeatedBuilder<Void, Void> repeatedBuilder() {
    return new NullRepeatedBuilder();
  }

  private static class NullBranchBuilder implements BranchBuilder<Void> {
    @Override
    public void put(final String key, final Object value) {}

    @Override
    public Void build() {
      return null;
    }
  }

  private static class NullRepeatedBuilder implements RepeatedBuilder<Void, Void> {
    @Override
    public void add(final Void unused) {}

    @Override
    public Void build() {
      return null;
    }
  }
}
