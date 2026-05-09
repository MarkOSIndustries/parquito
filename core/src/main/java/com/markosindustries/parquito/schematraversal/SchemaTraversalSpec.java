package com.markosindustries.parquito.schematraversal;

public interface SchemaTraversalSpec {
  boolean includesChild(int childFieldIndex);

  SchemaTraversalSpec forChild(int childFieldIndex);

  default SchemaTraversalSpec combineWith(final SchemaTraversalSpec other) {
    record CombinedSchemaTraversalSpecs(SchemaTraversalSpec a, SchemaTraversalSpec b)
        implements SchemaTraversalSpec {
      @Override
      public boolean includesChild(final int childFieldIndex) {
        return a.includesChild(childFieldIndex) || b.includesChild(childFieldIndex);
      }

      @Override
      public SchemaTraversalSpec forChild(final int childFieldIndex) {
        return new CombinedSchemaTraversalSpecs(
            a.forChild(childFieldIndex), b.forChild(childFieldIndex));
      }
    }
    ;

    return new CombinedSchemaTraversalSpecs(this, other);
  }
}
