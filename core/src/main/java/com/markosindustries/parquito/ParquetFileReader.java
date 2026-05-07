package com.markosindustries.parquito;

public class ParquetFileReader<Value, Repeated> {
  private final ParquetSchemaNode.Root schemaRoot;
  private final RowReadSpec<Repeated, Value> rowReadSpec;

  public ParquetFileReader(
      final ParquetSchemaNode.Root schemaRoot, final RowReadSpec<Repeated, Value> rowReadSpec) {
    this.schemaRoot = schemaRoot;
    this.rowReadSpec = rowReadSpec;
  }
}
