package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;

public class JSONReader implements Reader<JSONArray, JSONObject> {
  private final ParquetSchemaNode parquetSchemaNode;

  public JSONReader(final ParquetSchemaNode parquetSchemaNode) {
    this.parquetSchemaNode = parquetSchemaNode;
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldId) {
    return new JSONReader(parquetSchemaNode.getChild(childFieldId));
  }

  @Override
  public BranchBuilder<JSONObject> branchBuilder() {
    return new JSONBranchBuilder(parquetSchemaNode);
  }

  @Override
  public RepeatedBuilder<JSONArray, JSONObject> repeatedBuilder() {
    return new JSONRepeatedBuilder();
  }
}
