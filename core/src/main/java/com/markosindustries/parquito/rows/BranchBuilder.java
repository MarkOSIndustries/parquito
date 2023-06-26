package com.markosindustries.parquito.rows;

public interface BranchBuilder<Branch> {
  void put(String key, Object value);

  Branch build();
}
