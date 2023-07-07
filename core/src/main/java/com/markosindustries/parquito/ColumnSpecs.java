package com.markosindustries.parquito;

import java.util.Arrays;

public final class ColumnSpecs {
  private static final class All implements ColumnSpec {
    @Override
    public boolean includesChild(final String child) {
      return true;
    }

    @Override
    public ColumnSpec forChild(final String child) {
      return this;
    }
  }

  public static final All ALL = new All();

  public static ColumnSpec all() {
    return ALL;
  }

  private static final class None implements ColumnSpec {
    @Override
    public boolean includesChild(final String child) {
      return false;
    }

    @Override
    public ColumnSpec forChild(final String child) {
      return this;
    }
  }

  public static final None NONE = new None();

  public static ColumnSpec none() {
    return NONE;
  }

  private record Column(String[] path, int offset) implements ColumnSpec {
    @Override
    public boolean includesChild(final String child) {
      return path[offset].equals(child);
    }

    @Override
    public ColumnSpec forChild(final String child) {
      if (!includesChild(child)) {
        return NONE;
      }
      if (path.length > offset + 1) {
        return new Column(path, offset + 1);
      }
      return ALL;
    }
  }

  public static ColumnSpec column(final String... path) {
    if (path.length > 0) {
      return new Column(path, 0);
    }
    return ALL;
  }

  private record Union(ColumnSpec... columnSpecs) implements ColumnSpec {
    @Override
    public boolean includesChild(final String child) {
      return Arrays.stream(columnSpecs).anyMatch(columnSpec -> columnSpec.includesChild(child));
    }

    @Override
    public ColumnSpec forChild(final String child) {
      final var childSpecs =
          Arrays.stream(columnSpecs)
              .filter(columnSpec -> columnSpec.includesChild(child))
              .map(columnSpec -> columnSpec.forChild(child))
              .toArray(ColumnSpec[]::new);
      if (childSpecs.length > 0) {
        return new Union(childSpecs);
      }
      return NONE;
    }
  }

  public static ColumnSpec union(ColumnSpec... columnSpecs) {
    return new Union(columnSpecs);
  }
}
