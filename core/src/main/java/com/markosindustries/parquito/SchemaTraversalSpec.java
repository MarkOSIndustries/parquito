package com.markosindustries.parquito;

public interface SchemaTraversalSpec {
  boolean includesChild(int childFieldIndex);

  SchemaTraversalSpec forChild(int childFieldIndex);
}
