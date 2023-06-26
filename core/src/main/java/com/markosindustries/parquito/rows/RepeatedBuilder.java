package com.markosindustries.parquito.rows;

public interface RepeatedBuilder<Repeated, Value> {
  void add(Value value);

  Repeated build();
}
