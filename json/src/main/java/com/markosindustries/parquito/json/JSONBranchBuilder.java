package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.rows.BranchBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

class JSONBranchBuilder implements BranchBuilder<JSONObject> {
  private final JSONObject result;
  private final ParquetSchemaNode parquetSchemaNode;

  public JSONBranchBuilder(final ParquetSchemaNode parquetSchemaNode) {
    this.result = new JSONObject(parquetSchemaNode.getChildren().length);
    this.parquetSchemaNode = parquetSchemaNode;
  }

  @Override
  public void put(final int fieldIndex, final Object value) {
    final var schemaNode = parquetSchemaNode.getChildAtIndex(fieldIndex);

    if (value instanceof ByteBuffer) {
      result.put(
          schemaNode.getElement().name,
          StandardCharsets.UTF_8
              .decode(Base64.getEncoder().encode(((ByteBuffer) value).asReadOnlyBuffer()))
              .toString());
    } else {
      result.put(schemaNode.getElement().name, value);
    }
  }

  @Override
  public JSONObject build() {
    return result;
  }
}
