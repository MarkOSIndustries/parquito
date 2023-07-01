package com.markosindustries.parquito.json;

import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

public class JSONReader implements Reader<JSONArray, JSONObject> {
  @Override
  public Reader<?, ?> forChild(final String child) {
    return this;
  }

  @Override
  public BranchBuilder<JSONObject> branchBuilder() {
    return new JSONBranchBuilder();
  }

  @Override
  public RepeatedBuilder<JSONArray, JSONObject> repeatedBuilder() {
    return new JSONRepeatedBuilder();
  }
}
