package com.markosindustries.parquito;

import com.markosindustries.parquito.schematraversal.All;
import com.markosindustries.parquito.schematraversal.ExcludeAll;
import com.markosindustries.parquito.schematraversal.ExcludePath;
import com.markosindustries.parquito.schematraversal.IncludeAll;
import com.markosindustries.parquito.schematraversal.IncludePath;
import com.markosindustries.parquito.schematraversal.None;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.util.Arrays;

public final class SchemaTraversalSpecs {
  public static SchemaTraversalSpec all() {
    return All.INSTANCE;
  }

  public static SchemaTraversalSpec none() {
    return None.INSTANCE;
  }

  public static SchemaTraversalSpec includePath(final ParquetSchemaPath schemaPath) {
    if (schemaPath.path.length > 0) {
      return new IncludePath(schemaPath, 0);
    }
    return all();
  }

  public static SchemaTraversalSpec includeAll(final ParquetSchemaPath... schemaPaths) {
    return new IncludeAll(
        Arrays.stream(schemaPaths)
            .map(SchemaTraversalSpecs::includePath)
            .toArray(SchemaTraversalSpec[]::new));
  }

  public static SchemaTraversalSpec excludePath(final ParquetSchemaPath schemaPath) {
    if (schemaPath.path.length > 0) {
      return new ExcludePath(schemaPath, 0);
    }
    return none();
  }

  public static SchemaTraversalSpec excludeAll(final ParquetSchemaPath... schemaPaths) {
    return new ExcludeAll(
        Arrays.stream(schemaPaths)
            .map(SchemaTraversalSpecs::excludePath)
            .toArray(SchemaTraversalSpec[]::new));
  }
}
