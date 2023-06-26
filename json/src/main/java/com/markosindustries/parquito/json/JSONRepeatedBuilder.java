package com.markosindustries.parquito.json;

import com.markosindustries.parquito.rows.RepeatedBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

class JSONRepeatedBuilder implements RepeatedBuilder<JSONArray, JSONObject> {
  final JSONArray array = new JSONArray();

  @Override
  public void add(final JSONObject jsonObject) {
    array.put(jsonObject);
  }

  @Override
  public JSONArray build() {
    return array;
  }
}
