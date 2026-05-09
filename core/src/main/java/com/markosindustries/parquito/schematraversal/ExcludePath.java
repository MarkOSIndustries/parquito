package com.markosindustries.parquito.schematraversal;

import com.markosindustries.parquito.ParquetSchemaPath;

public record ExcludePath(ParquetSchemaPath schemaPath, int offset) implements SchemaTraversalSpec {
  @Override
  public boolean includesChild(final int childFieldIndex) {
    return schemaPath.getPathLength() > offset + 1
        || schemaPath.getFieldIndexAtDepth(offset) != childFieldIndex;
  }

  @Override
  public SchemaTraversalSpec forChild(final int childFieldIndex) {
    if (schemaPath.getFieldIndexAtDepth(offset) != childFieldIndex) {
      return All.INSTANCE;
    }
    if (schemaPath.getPathLength() > offset + 1) {
      return new ExcludePath(schemaPath, offset + 1);
    }
    return None.INSTANCE;
  }
}
