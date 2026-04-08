package com.markosindustries.parquito;

public interface ColumnSpec {
  boolean includesChild(int childFieldId);

  ColumnSpec forChild(int childFieldId);
}
