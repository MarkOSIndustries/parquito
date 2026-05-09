package com.markosindustries.parquito.schematraversal;

public final class None implements SchemaTraversalSpec {
  public static final None INSTANCE = new None();

  private None() {}

  @Override
  public boolean includesChild(final int childFieldIndex) {
    return false;
  }

  @Override
  public SchemaTraversalSpec forChild(final int childFieldIndex) {
    return this;
  }

  @Override
  public SchemaTraversalSpec combineWith(final SchemaTraversalSpec other) {
    return other;
  }
}
