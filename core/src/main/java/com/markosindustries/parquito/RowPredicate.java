package com.markosindustries.parquito;

public interface RowPredicate {
  boolean includesChild(String child);

  RowPredicate forChild(String child);

  RowPredicate ALL =
      new RowPredicate() {
        @Override
        public boolean includesChild(final String child) {
          return false;
        }

        @Override
        public RowPredicate forChild(final String child) {
          return this;
        }
      };
}
