package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Message;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.ArrayList;
import java.util.List;

class ProtobufRepeatedBuilder implements RepeatedBuilder<List<Message>, Message> {
  private final List<Message> list = new ArrayList<>();

  @Override
  public void add(final Message message) {
    list.add(message);
  }

  @Override
  public List<Message> build() {
    return list;
  }
}
