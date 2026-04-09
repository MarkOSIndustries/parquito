package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;

public class JSONReader implements Reader<JSONArray, JSONObject> {
  private final ParquetSchemaNode parquetSchemaNode;
  private final JSONReader[] childReadersByIndex;

  public JSONReader(final ParquetSchemaNode parquetSchemaNode) {
    this.parquetSchemaNode = parquetSchemaNode;
    this.childReadersByIndex = new JSONReader[parquetSchemaNode.getChildren().length];
    for (var childIndex = 0; childIndex < parquetSchemaNode.getChildren().length; childIndex++) {
      childReadersByIndex[childIndex] =
          new JSONReader(parquetSchemaNode.getChildAtIndex(childIndex));
    }
  }

  @Override
  public Reader<?, ?> forChild(final int childIndex) {
    return childReadersByIndex[childIndex];
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
