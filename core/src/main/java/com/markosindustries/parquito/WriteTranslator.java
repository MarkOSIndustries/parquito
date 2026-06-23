package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchAccumulator;

public interface WriteTranslator<Branch> {
  void translate(Branch branch, BranchAccumulator accumulator);

  default void translateUnsafe(Object branch, BranchAccumulator accumulator) {
    //noinspection unchecked
    translate((Branch) branch, accumulator);
  }
}
