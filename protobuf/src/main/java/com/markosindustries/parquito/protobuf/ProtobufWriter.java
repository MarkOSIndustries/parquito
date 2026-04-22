package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.Writer;
import java.util.List;
import org.apache.parquet.format.SchemaElement;

public class ProtobufWriter<Value extends Message> implements Writer<Value> {
  private final ParquetSchemaNode.Root schemaRoot;
  private final List<SchemaElement> rawSchema;
  private final ProtobufMessageWriteTranslator<Value> translator;

  public static <Value extends Message> ProtobufWriter<Value> fromDescriptor(
      final Descriptors.Descriptor descriptor, final ProtobufParquetConfig protobufParquetConfig) {
    return new ProtobufWriter<>(
        descriptor,
        new ProtobufSchemaConverter(protobufParquetConfig).convertDescriptorToSchema(descriptor));
  }

  private ProtobufWriter(
      final Descriptors.Descriptor descriptor, final List<SchemaElement> schemaElements) {
    this.rawSchema = schemaElements;
    this.schemaRoot = ParquetSchemaNode.from(rawSchema);
    this.translator = new ProtobufMessageWriteTranslator<>(descriptor, schemaRoot);
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
  public WriteTranslator<Value, ?> getTranslator() {
    return translator;
  }
}
