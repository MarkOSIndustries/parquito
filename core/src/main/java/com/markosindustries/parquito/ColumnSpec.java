package com.markosindustries.parquito;

public interface ColumnSpec {
  boolean includesChild(String child);

  ColumnSpec forChild(String child);
}
