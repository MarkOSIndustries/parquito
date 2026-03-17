package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.rows.RepeatedBuilder;

class JSONRepeatedBuilder implements RepeatedBuilder<JSONArray, JSONObject> {
  final JSONArray array = new JSONArray();

  @Override
  public void add(final JSONObject jsonObject) {
    array.add(jsonObject);
  }

  @Override
  public JSONArray build() {
    return array;
  }
}
