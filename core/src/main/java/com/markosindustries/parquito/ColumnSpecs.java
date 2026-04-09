package com.markosindustries.parquito;

import java.util.Arrays;

public final class ColumnSpecs {
  private static final class All implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return true;
    }

    @Override
    public ColumnSpec forChild(final int childFieldIndex) {
      return this;
    }
  }

  private static final All ALL = new All();

  public static ColumnSpec all() {
    return ALL;
  }

  private static final class None implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return false;
    }

    @Override
    public ColumnSpec forChild(final int childFieldIndex) {
      return this;
    }
  }

  private static final None NONE = new None();

  public static ColumnSpec none() {
    return NONE;
  }

  private record Column(ParquetSchemaPath schemaPath, int offset) implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return schemaPath.pathAsFieldIndices[offset] == childFieldIndex;
    }

    @Override
    public ColumnSpec forChild(final int childFieldIndex) {
      if (!includesChild(childFieldIndex)) {
        return NONE;
      }
      if (schemaPath.path.length > offset + 1) {
        return new Column(schemaPath, offset + 1);
      }
      return ALL;
    }
  }

  public static ColumnSpec column(final ParquetSchemaPath schemaPath) {
    if (schemaPath.path.length > 0) {
      return new Column(schemaPath, 0);
    }
    return ALL;
  }

  private record Union(ColumnSpec... columnSpecs) implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return Arrays.stream(columnSpecs)
          .anyMatch(columnSpec -> columnSpec.includesChild(childFieldIndex));
    }

    @Override
    public ColumnSpec forChild(final int childFieldIndex) {
      final var childSpecs =
          Arrays.stream(columnSpecs)
              .filter(columnSpec -> columnSpec.includesChild(childFieldIndex))
              .map(columnSpec -> columnSpec.forChild(childFieldIndex))
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
