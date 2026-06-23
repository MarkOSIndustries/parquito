package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.DataPageReader;

public interface DataPageCursor {
  ParquetSchemaNode getSchemaNode();

  DataPageReader getDataPage();

  int getDefinitionIndex();

  int getValueIndex();
}
