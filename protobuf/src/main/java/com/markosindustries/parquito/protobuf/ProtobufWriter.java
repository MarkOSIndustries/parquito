package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.Writer;
import java.util.List;
import org.apache.parquet.format.SchemaElement;

public class ProtobufWriter implements Writer<Message> {
  private final ParquetSchemaNode.Root schemaRoot;
  private final List<SchemaElement> rawSchema;
  private final ProtobufMessageWriteTranslator translator;

  public static ProtobufWriter fromDescriptor(
      final Descriptors.Descriptor descriptor, final ProtobufParquetConfig protobufParquetConfig) {
    return new ProtobufWriter(
        descriptor,
        new ProtobufSchemaConverter(protobufParquetConfig).convertDescriptorToSchema(descriptor));
  }

  public ProtobufWriter(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode.Root schema) {
    this.rawSchema = schema.toRawSchema();
    this.schemaRoot = schema;
    this.translator = new ProtobufMessageWriteTranslator(descriptor, schemaRoot);
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
  public WriteTranslator<Message> getTranslator() {
    return translator;
  }
}
