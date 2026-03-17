package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.rows.BranchBuilder;

class JSONBranchBuilder implements BranchBuilder<JSONObject> {
  final JSONObject result = new JSONObject();

  @Override
  public void put(final String key, final Object value) {
    result.put(key, value);
  }

  @Override
  public JSONObject build() {
    return result;
  }
}
