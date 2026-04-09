package com.markosindustries.parquito;

public interface ColumnSpec {
  boolean includesChild(int childFieldIndex);

  ColumnSpec forChild(int childFieldIndex);
}
