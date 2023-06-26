package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapReader implements Reader<List<Map<String, Object>>, Map<String, Object>> {
  @Override
  public Reader<?, ?> getChild(final String child) {
    return this;
  }

  @Override
  public BranchBuilder<Map<String, Object>> branchBuilder() {
    return new MapBranchBuilder();
  }

  @Override
  public RepeatedBuilder<List<Map<String, Object>>, Map<String, Object>> repeatedBuilder() {
    return new MapRepeatedBuilder();
  }

  private static class MapBranchBuilder implements BranchBuilder<Map<String, Object>> {
    private final Map<String, Object> map = new HashMap<>();

    @Override
    public void put(final String key, final Object value) {
      map.put(key, value);
    }

    @Override
    public Map<String, Object> build() {
      return map;
    }
  }

  private static class MapRepeatedBuilder
      implements RepeatedBuilder<List<Map<String, Object>>, Map<String, Object>> {
    private final ArrayList<Map<String, Object>> list = new ArrayList<>();

    @Override
    public void add(final Map<String, Object> value) {
      list.add(value);
    }

    @Override
    public List<Map<String, Object>> build() {
      return list;
    }
  }
}
