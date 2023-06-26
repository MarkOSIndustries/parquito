package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;

public interface Reader<Collection, Value> {
  Reader<?, ?> getChild(String child);

  BranchBuilder<Value> branchBuilder();

  RepeatedBuilder<Collection, Value> repeatedBuilder();
}
