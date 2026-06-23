package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.BranchAccumulator;
import java.util.List;
import org.apache.parquet.format.SchemaElement;

class NoOpWriter implements Writer<Object> {
  private final List<SchemaElement> rawSchema;
  private final ParquetSchemaNode.Root schemaRoot;

  public NoOpWriter(final List<SchemaElement> rawSchema, final ParquetSchemaNode.Root schemaRoot) {
    this.rawSchema = rawSchema;
    this.schemaRoot = schemaRoot;
  }

  @Override
  public List<? extends SchemaElement> getRawSchema() {
    return rawSchema;
  }

  @Override
  public ParquetSchemaNode.Root getSchemaRoot() {
    return schemaRoot;
  }

  @Override
  public WriteTranslator<Object> getTranslator() {
    return NoOpWriteTranslator.INSTANCE;
  }

  private static class NoOpWriteTranslator implements WriteTranslator<Object> {
    public static final NoOpWriteTranslator INSTANCE = new NoOpWriteTranslator();

    @Override
    public void translate(final Object o, final BranchAccumulator accumulator) {}
  }
}
