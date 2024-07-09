package com.markosindustries.parquito;

import org.apache.parquet.format.FileMetaData;

public class ParquetFileReader {
  private final FileMetaData footer;
  private final ParquetSchemaNode.Root schemaRoot;

  public ParquetFileReader(FileMetaData footer) {
    this.footer = footer;
    this.schemaRoot = ParquetSchemaNode.from(footer.schema);
  }
}
