package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.rows.BranchBuilder;

class JSONBranchBuilder implements BranchBuilder<JSONObject> {
  private final JSONObject result;
  private final ParquetSchemaNode parquetSchemaNode;

  public JSONBranchBuilder(final ParquetSchemaNode parquetSchemaNode) {
    this.result = new JSONObject(parquetSchemaNode.getChildFieldIds().size());
    this.parquetSchemaNode = parquetSchemaNode;
  }

  @Override
  public void put(final int fieldId, final Object value) {
    result.put(parquetSchemaNode.getChild(fieldId).getElement().name, value);
  }

  @Override
  public JSONObject build() {
    return result;
  }
}
