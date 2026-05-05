package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaBuilder;
import com.markosindustries.parquito.SchemaTraversalSpec;
import java.util.List;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;

public class ProtobufSchemaConverter {
  private final ProtobufParquetConfig protobufParquetConfig;

  public ProtobufSchemaConverter(ProtobufParquetConfig protobufParquetConfig) {
    this.protobufParquetConfig = protobufParquetConfig;
  }

  public List<SchemaElement> convertDescriptorToSchema(
      final Descriptors.Descriptor descriptor, final SchemaTraversalSpec schemaTraversalSpec) {
    return convertMessage(
            new ParquetSchemaBuilder(descriptor.getFullName()),
            descriptor,
            false,
            schemaTraversalSpec,
            protobufParquetConfig.recursionLimit())
        .build();
  }

  private ParquetSchemaBuilder convertMessage(
      final ParquetSchemaBuilder builder,
      final Descriptors.Descriptor descriptor,
      final boolean isMapEntry,
      final SchemaTraversalSpec schemaTraversalSpec,
      final int recursionLimit) {
    var childrenCount = 0;
    if (recursionLimit > 0) {
      final var fields = descriptor.getFields();
      for (int childFieldIndex = 0; childFieldIndex < fields.size(); childFieldIndex++) {
        if (schemaTraversalSpec.includesChild(childFieldIndex)) {
          childrenCount++;
          final Descriptors.FieldDescriptor field = fields.get(childFieldIndex);
          builder.addChild(
              convertField(
                  field,
                  isMapEntry || field.isRequired(),
                  schemaTraversalSpec.forChild(childFieldIndex),
                  recursionLimit - 1));
        }
      }
    }

    final var finalChildrenCount = childrenCount;
    return builder.mutateElement(
        schemaElement -> {
          return schemaElement.setNum_children(finalChildrenCount);
        });
  }

  private ParquetSchemaBuilder convertField(
      final Descriptors.FieldDescriptor field,
      final boolean isRequired,
      final SchemaTraversalSpec schemaTraversalSpec,
      final int recursionLimit) {
    final var topLevelBuilder = new ParquetSchemaBuilder(field.getName());
    topLevelBuilder.mutateElement(
        schemaElement -> {
          return schemaElement
              .setField_id(field.getNumber())
              .setRepetition_type(
                  isRequired ? FieldRepetitionType.REQUIRED : FieldRepetitionType.OPTIONAL);
        });

    final var protobufAlignedBuilder = wrapListsAndMaps(topLevelBuilder, field);
    if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
      convertMessage(
          protobufAlignedBuilder,
          field.getMessageType(),
          field.isMapField(),
          schemaTraversalSpec,
          recursionLimit);
    } else {
      protobufAlignedBuilder.mutateElement(
          schemaElement ->
              switch (field.getType()) {
                case BYTES -> schemaElement.setType(Type.BYTE_ARRAY);
                case FLOAT -> schemaElement.setType(Type.FLOAT);
                case DOUBLE -> schemaElement.setType(Type.DOUBLE);
                case INT64, SINT64, SFIXED64 ->
                    schemaElement
                        .setType(Type.INT64)
                        .setConverted_type(ConvertedType.INT_64)
                        .setLogicalType(LogicalType.INTEGER(new IntType((byte) 64, true)));
                case UINT64, FIXED64 ->
                    schemaElement
                        .setType(Type.INT64)
                        .setConverted_type(ConvertedType.UINT_64)
                        .setLogicalType(LogicalType.INTEGER(new IntType((byte) 64, false)));
                case INT32, SINT32, SFIXED32 ->
                    schemaElement
                        .setType(Type.INT32)
                        .setConverted_type(ConvertedType.INT_32)
                        .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, true)));
                case UINT32, FIXED32 ->
                    schemaElement
                        .setType(Type.INT32)
                        .setConverted_type(ConvertedType.UINT_32)
                        .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, false)));
                case BOOL -> schemaElement.setType(Type.BOOLEAN);
                case STRING ->
                    schemaElement
                        .setType(Type.BYTE_ARRAY)
                        .setConverted_type(ConvertedType.UTF8)
                        .setLogicalType(LogicalType.STRING(new StringType()));
                case GROUP -> throw new UnsupportedOperationException("Can't handle groups");
                case ENUM -> {
                  if (protobufParquetConfig.enumsAsInt32()) {
                    yield schemaElement
                        .setType(Type.INT32)
                        .setConverted_type(ConvertedType.ENUM)
                        .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, false)));
                  } else {
                    yield schemaElement
                        .setType(Type.BYTE_ARRAY)
                        .setConverted_type(ConvertedType.ENUM)
                        .setLogicalType(LogicalType.ENUM(new EnumType()));
                  }
                }
                case MESSAGE ->
                    throw new UnsupportedOperationException("We should never get to this branch");
              });
    }

    return topLevelBuilder;
  }

  private ParquetSchemaBuilder wrapListsAndMaps(
      final ParquetSchemaBuilder topLevelBuilder, final Descriptors.FieldDescriptor field) {
    if (field.isMapField()) {
      final var keyValue =
          new ParquetSchemaBuilder("key_value")
              .mutateElement(kv -> kv.setRepetition_type(FieldRepetitionType.REPEATED));
      topLevelBuilder
          .mutateElement(
              schemaElement ->
                  schemaElement
                      .setConverted_type(ConvertedType.MAP)
                      .setLogicalType(LogicalType.MAP(new MapType())))
          .addChild(keyValue);
      return keyValue;
    } else if (field.isRepeated()) {
      final var element =
          new ParquetSchemaBuilder("element")
              .mutateElement(elem -> elem.setRepetition_type(FieldRepetitionType.REQUIRED));
      final var list =
          new ParquetSchemaBuilder("list")
              .mutateElement(kv -> kv.setRepetition_type(FieldRepetitionType.REPEATED))
              .addChild(element);
      topLevelBuilder
          .mutateElement(
              schemaElement ->
                  schemaElement
                      .setConverted_type(ConvertedType.LIST)
                      .setLogicalType(LogicalType.LIST(new ListType())))
          .addChild(list);
      return element;
    } else {
      return topLevelBuilder;
    }
  }
}
