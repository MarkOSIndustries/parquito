package com.markosindustries.parquito.rows;

public interface ParquetFieldAccumulator {
  void beginBranch();

  void endBranch(final int newRepetitionLevel);

  void accumulateNull();

  void accumulateNull(int repetitionLevel, int definitionLevel);
}
