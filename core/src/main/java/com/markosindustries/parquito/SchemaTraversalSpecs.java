package com.markosindustries.parquito;

import java.util.Arrays;

public final class SchemaTraversalSpecs {
  private static final class All implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return true;
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      return this;
    }
  }

  private static final All ALL = new All();

  public static SchemaTraversalSpec all() {
    return ALL;
  }

  private static final class None implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return false;
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      return this;
    }
  }

  private static final None NONE = new None();

  public static SchemaTraversalSpec none() {
    return NONE;
  }

  private record IncludePath(ParquetSchemaPath schemaPath, int offset)
      implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return schemaPath.pathAsFieldIndices[offset] == childFieldIndex;
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      if (!includesChild(childFieldIndex)) {
        return NONE;
      }
      if (schemaPath.path.length > offset + 1) {
        return new IncludePath(schemaPath, offset + 1);
      }
      return ALL;
    }
  }

  public static SchemaTraversalSpec includePath(final ParquetSchemaPath schemaPath) {
    if (schemaPath.path.length > 0) {
      return new IncludePath(schemaPath, 0);
    }
    return ALL;
  }

  private record IncludeAll(SchemaTraversalSpec... includePaths) implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return Arrays.stream(includePaths)
          .anyMatch(includePath -> includePath.includesChild(childFieldIndex));
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      final var childSpecs =
          Arrays.stream(includePaths)
              .filter(includePath -> includePath.includesChild(childFieldIndex))
              .map(includePath -> includePath.forChild(childFieldIndex))
              .toArray(SchemaTraversalSpec[]::new);
      if (childSpecs.length > 0) {
        return new IncludeAll(childSpecs);
      }
      return NONE;
    }
  }

  public static SchemaTraversalSpec includeAll(final ParquetSchemaPath... schemaPaths) {
    return new IncludeAll(
        Arrays.stream(schemaPaths)
            .map(SchemaTraversalSpecs::includePath)
            .toArray(SchemaTraversalSpec[]::new));
  }

  private record ExcludePath(ParquetSchemaPath schemaPath, int offset)
      implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return true;
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      if (schemaPath.pathAsFieldIndices[offset] != childFieldIndex) {
        return ALL;
      }
      if (schemaPath.path.length > offset + 1) {
        return new ExcludePath(schemaPath, offset + 1);
      }
      return NONE;
    }
  }

  public static SchemaTraversalSpec excludePath(final ParquetSchemaPath schemaPath) {
    if (schemaPath.path.length > 0) {
      return new ExcludePath(schemaPath, 0);
    }
    return NONE;
  }

  private record ExcludeAll(SchemaTraversalSpec... includePaths) implements SchemaTraversalSpec {
    @Override
    public boolean includesChild(final int childFieldIndex) {
      return Arrays.stream(includePaths)
          .allMatch(includePath -> includePath.includesChild(childFieldIndex));
    }

    @Override
    public SchemaTraversalSpec forChild(final int childFieldIndex) {
      final var childSpecs =
          Arrays.stream(includePaths)
              .filter(includePath -> includePath.includesChild(childFieldIndex))
              .map(includePath -> includePath.forChild(childFieldIndex))
              .toArray(SchemaTraversalSpec[]::new);
      if (childSpecs.length > 0) {
        return new ExcludeAll(childSpecs);
      }
      return ALL;
    }
  }

  public static SchemaTraversalSpec excludeAll(final ParquetSchemaPath... schemaPaths) {
    return new ExcludeAll(
        Arrays.stream(schemaPaths)
            .map(SchemaTraversalSpecs::excludePath)
            .toArray(SchemaTraversalSpec[]::new));
  }
}
