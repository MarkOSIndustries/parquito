package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;

public class IdentityBranchBuilder<Branch> implements BranchBuilder<Branch> {
  private Branch value;

  @Override
  public void put(final int childFieldIndex, final Object value) {
    //noinspection unchecked
    this.value = (Branch) value;
  }

  @Override
  public Branch build() {
    return value;
  }
}
