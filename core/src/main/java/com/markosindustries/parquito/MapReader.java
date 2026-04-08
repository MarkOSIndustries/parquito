package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapReader implements Reader<List<Map<String, Object>>, Map<String, Object>> {
  private final ParquetSchemaNode parquetSchemaNode;

  public MapReader(final ParquetSchemaNode parquetSchemaNode) {
    this.parquetSchemaNode = parquetSchemaNode;
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldId) {
    return this;
  }

  @Override
  public BranchBuilder<Map<String, Object>> branchBuilder() {
    return new MapBranchBuilder(parquetSchemaNode);
  }

  @Override
  public RepeatedBuilder<List<Map<String, Object>>, Map<String, Object>> repeatedBuilder() {
    return new MapRepeatedBuilder();
  }

  private static class MapBranchBuilder implements BranchBuilder<Map<String, Object>> {
    private final Map<String, Object> map = new HashMap<>();
    private final ParquetSchemaNode parquetSchemaNode1;

    public MapBranchBuilder(final ParquetSchemaNode parquetSchemaNode) {
      parquetSchemaNode1 = parquetSchemaNode;
    }

    @Override
    public void put(final int fieldId, final Object value) {
      map.put(parquetSchemaNode1.getChild(fieldId).getElement().name, value);
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
