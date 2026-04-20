package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Writer;
import java.util.List;
import org.apache.parquet.format.SchemaElement;

public class ProtobufWriter<Value extends Message> implements Writer<Value> {

  private final Descriptors.Descriptor descriptor;
  private final ParquetSchemaNode.Root schemaRoot;
  private final List<SchemaElement> rawSchema;

  public static <Value extends Message> ProtobufWriter<Value> fromDescriptor(
      final Descriptors.Descriptor descriptor, final ProtobufParquetConfig protobufParquetConfig) {
    return new ProtobufWriter<>(
        descriptor,
        new ProtobufSchemaConverter(protobufParquetConfig).convertDescriptorToSchema(descriptor));
  }

  private ProtobufWriter(
      final Descriptors.Descriptor descriptor, final List<SchemaElement> schemaElements) {
    this.descriptor = descriptor;
    this.rawSchema = schemaElements;
    this.schemaRoot = ParquetSchemaNode.from(rawSchema);
  }

  @Override
  public Shredder<Value> makeShredder(final WriteAccumulator writeAccumulator) {
    return new ProtobufShredder<>(descriptor, schemaRoot, writeAccumulator);
  }

  @Override
  public List<? extends SchemaElement> getRawSchema() {
    return rawSchema;
  }

  @Override
  public ParquetSchemaNode.Root getSchemaRoot() {
    return schemaRoot;
  }
}
