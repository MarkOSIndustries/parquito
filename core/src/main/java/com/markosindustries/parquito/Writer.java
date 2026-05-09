package com.markosindustries.parquito;

import java.util.List;
import org.apache.parquet.format.SchemaElement;

public interface Writer<Value> {
  List<? extends SchemaElement> getRawSchema();

  ParquetSchemaNode.Root getSchemaRoot();

  WriteTranslator<Value, ?> getTranslator();
}
