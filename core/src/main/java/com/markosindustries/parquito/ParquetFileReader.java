package com.markosindustries.parquito;

public class ParquetFileReader<Row> {
  private final ParquetSchemaNode.Root schemaRoot;
  private final RowReadSpec<Row> rowReadSpec;

  public ParquetFileReader(
      final ParquetSchemaNode.Root schemaRoot, final RowReadSpec<Row> rowReadSpec) {
    this.schemaRoot = schemaRoot;
    this.rowReadSpec = rowReadSpec;
  }
}
