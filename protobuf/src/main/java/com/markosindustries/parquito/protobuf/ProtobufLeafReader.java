package com.markosindustries.parquito.protobuf;

import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.ArrayList;
import java.util.List;

public class ProtobufLeafReader implements Reader<List<Object>, Object> {
  public static final ProtobufLeafReader INSTANCE = new ProtobufLeafReader();

  private ProtobufLeafReader() {}

  @Override
  public Reader<?, ?> forChild(final String child) {
    throw new UnsupportedOperationException("Leaf nodes don't have children");
  }

  @Override
  public BranchBuilder<Object> branchBuilder() {
    throw new UnsupportedOperationException("Leaf nodes don't have branches");
  }

  @Override
  public RepeatedBuilder<List<Object>, Object> repeatedBuilder() {
    return new ListRepeatedBuilder();
  }

  private static class ListRepeatedBuilder implements RepeatedBuilder<List<Object>, Object> {
    private final List<Object> list = new ArrayList<>();

    @Override
    public void add(final Object value) {
      list.add(value);
    }

    @Override
    public List<Object> build() {
      return list;
    }
  }
}
