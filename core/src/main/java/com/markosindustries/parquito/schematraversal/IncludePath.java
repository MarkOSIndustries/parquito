package com.markosindustries.parquito.schematraversal;

import com.markosindustries.parquito.ParquetSchemaPath;

public record IncludePath(ParquetSchemaPath schemaPath, int offset) implements SchemaTraversalSpec {
  @Override
  public boolean includesChild(final int childFieldIndex) {
    return schemaPath.getFieldIndexAtDepth(offset) == childFieldIndex;
  }

  @Override
  public SchemaTraversalSpec forChild(final int childFieldIndex) {
    if (!includesChild(childFieldIndex)) {
      return None.INSTANCE;
    }
    if (schemaPath.getPathLength() > offset + 1) {
      return new IncludePath(schemaPath, offset + 1);
    }
    return All.INSTANCE;
  }
}
