package com.markosindustries.parquito;

import java.util.Arrays;

public final class ColumnSpecs {
  private static final class All implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldId) {
      return true;
    }

    @Override
    public ColumnSpec forChild(final int childFieldId) {
      return this;
    }
  }

  private static final All ALL = new All();

  public static ColumnSpec all() {
    return ALL;
  }

  private static final class None implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldId) {
      return false;
    }

    @Override
    public ColumnSpec forChild(final int childFieldId) {
      return this;
    }
  }

  private static final None NONE = new None();

  public static ColumnSpec none() {
    return NONE;
  }

  private record Column(ParquetSchemaPath schemaPath, int offset) implements ColumnSpec {
    @Override
    public boolean includesChild(final int childFieldId) {
      return schemaPath.path[offset].field_id == childFieldId;
    }

    @Override
    public ColumnSpec forChild(final int childFieldId) {
      if (!includesChild(childFieldId)) {
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
    public boolean includesChild(final int childFieldId) {
      return Arrays.stream(columnSpecs)
          .anyMatch(columnSpec -> columnSpec.includesChild(childFieldId));
    }

    @Override
    public ColumnSpec forChild(final int childFieldId) {
      final var childSpecs =
          Arrays.stream(columnSpecs)
              .filter(columnSpec -> columnSpec.includesChild(childFieldId))
              .map(columnSpec -> columnSpec.forChild(childFieldId))
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
