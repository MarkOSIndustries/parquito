package com.markosindustries.parquito.schematraversal;

import java.util.Arrays;

public record ExcludeAll(SchemaTraversalSpec... includePaths) implements SchemaTraversalSpec {
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
    return All.INSTANCE;
  }
}
