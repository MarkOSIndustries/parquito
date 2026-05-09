package com.markosindustries.parquito.schematraversal;

public final class All implements SchemaTraversalSpec {
  public static final All INSTANCE = new All();

  private All() {}

  @Override
  public boolean includesChild(final int childFieldIndex) {
    return true;
  }

  @Override
  public SchemaTraversalSpec forChild(final int childFieldIndex) {
    return this;
  }

  @Override
  public SchemaTraversalSpec combineWith(final SchemaTraversalSpec other) {
    return this;
  }
}
