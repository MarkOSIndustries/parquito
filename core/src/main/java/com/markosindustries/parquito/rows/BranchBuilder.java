package com.markosindustries.parquito.rows;

public interface BranchBuilder<Branch> {
  void put(int childFieldId, Object value);

  Branch build();
}
