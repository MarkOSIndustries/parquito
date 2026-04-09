package com.markosindustries.parquito.rows;

public interface BranchBuilder<Branch> {
  void put(int childFieldIndex, Object value);

  Branch build();
}
