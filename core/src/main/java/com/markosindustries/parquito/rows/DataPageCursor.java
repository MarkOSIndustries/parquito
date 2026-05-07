package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.DataPageReader;

public interface DataPageCursor<Value> {
  ParquetSchemaNode getSchemaNode();

  DataPageReader<Value> getDataPage();

  int getDefinitionIndex();

  int getValueIndex();
}
