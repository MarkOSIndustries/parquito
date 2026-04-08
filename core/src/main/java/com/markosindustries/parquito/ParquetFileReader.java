package com.markosindustries.parquito;

public class ParquetFileReader<ReadAs, Value, Repeated> {
  private final ParquetSchemaNode.Root schemaRoot;
  private final RowReadSpec<Repeated, Value, ReadAs> rowReadSpec;

  public ParquetFileReader(
      final ParquetSchemaNode.Root schemaRoot,
      final RowReadSpec<Repeated, Value, ReadAs> rowReadSpec) {
    this.schemaRoot = schemaRoot;
    this.rowReadSpec = rowReadSpec;
  }
}
