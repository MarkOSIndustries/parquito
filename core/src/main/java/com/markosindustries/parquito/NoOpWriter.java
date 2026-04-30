package com.markosindustries.parquito;

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
  public WriteTranslator<Object, ?> getTranslator() {
    return NoOpWriteTranslator.INSTANCE;
  }

  private static class NoOpWriteTranslator implements WriteTranslator<Object, Object> {
    public static final NoOpWriteTranslator INSTANCE = new NoOpWriteTranslator();

    @Override
    public Object getField(final int childIndex, final Object o) {
      return null;
    }

    @Override
    public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
      return this;
    }

    @Override
    public Object translate(final Object o) {
      return o;
    }
  }
}
