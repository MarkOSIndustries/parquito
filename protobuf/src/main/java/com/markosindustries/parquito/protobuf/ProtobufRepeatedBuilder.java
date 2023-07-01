package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Message;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.ArrayList;
import java.util.List;

class ProtobufRepeatedBuilder<M extends Message> implements RepeatedBuilder<List<M>, M> {
  private final List<M> list = new ArrayList<>();

  @Override
  public void add(final M message) {
    list.add(message);
  }

  @Override
  public List<M> build() {
    return list;
  }
}
