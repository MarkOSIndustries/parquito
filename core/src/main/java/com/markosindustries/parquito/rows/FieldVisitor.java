package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.page.Values;

public interface FieldVisitor extends Values.Visitor {
  FieldVisitor forChildIndex(int childIndex);

  void endBranch();

  void endRepeated();
}
