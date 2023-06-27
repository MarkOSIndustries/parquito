package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaBuilder;
import java.util.List;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;

public class ProtobufSchemaConverter {
  private final ProtobufParquetConfig protobufParquetConfig;

  public ProtobufSchemaConverter(ProtobufParquetConfig protobufParquetConfig) {
    this.protobufParquetConfig = protobufParquetConfig;
  }

  public List<SchemaElement> blah(Descriptors.Descriptor descriptor) {
    return convertMessage(new ParquetSchemaBuilder(descriptor.getFullName()), descriptor).build();
  }

  private ParquetSchemaBuilder convertField(final Descriptors.FieldDescriptor field) {
    final var builder =
        new ParquetSchemaBuilder(field.getName())
            .mutateElement(
                schemaElement -> {
                  return schemaElement
                      .setField_id(field.getNumber())
                      .setRepetition_type(
                          field.isRepeated()
                              ? FieldRepetitionType.REPEATED
                              : field.isRequired()
                                  ? FieldRepetitionType.REQUIRED
                                  : FieldRepetitionType.OPTIONAL);
                });

    return switch (field.getType()) {
      case BYTES -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement.setType(Type.BYTE_ARRAY);
            });
      }
      case FLOAT -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement.setType(Type.FLOAT);
            });
      }
      case DOUBLE -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement.setType(Type.DOUBLE);
            });
      }
      case INT64, SINT64, SFIXED64 -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement
                  .setType(Type.INT64)
                  .setConverted_type(ConvertedType.INT_64)
                  .setLogicalType(LogicalType.INTEGER(new IntType((byte) 64, true)));
            });
      }
      case UINT64, FIXED64 -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement
                  .setType(Type.INT64)
                  .setConverted_type(ConvertedType.UINT_64)
                  .setLogicalType(LogicalType.INTEGER(new IntType((byte) 64, false)));
            });
      }
      case INT32, SINT32, SFIXED32 -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement
                  .setType(Type.INT32)
                  .setConverted_type(ConvertedType.INT_32)
                  .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, true)));
            });
      }
      case UINT32, FIXED32 -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement
                  .setType(Type.INT32)
                  .setConverted_type(ConvertedType.UINT_32)
                  .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, false)));
            });
      }
      case BOOL -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement.setType(Type.BOOLEAN);
            });
      }
      case STRING -> {
        yield builder.mutateElement(
            schemaElement -> {
              return schemaElement
                  .setType(Type.BYTE_ARRAY)
                  .setConverted_type(ConvertedType.UTF8)
                  .setLogicalType(LogicalType.STRING(new StringType()));
            });
      }
      case GROUP -> {
        throw new UnsupportedOperationException("Can't handle groups");
      }
      case MESSAGE -> {
        yield convertMessage(builder, field.getMessageType());
      }
      case ENUM -> {
        yield builder.mutateElement(
            schemaElement -> {
              if (protobufParquetConfig.enumsAsInt32()) {
                return schemaElement
                    .setType(Type.INT32)
                    .setLogicalType(LogicalType.INTEGER(new IntType((byte) 32, false)));
              } else {
                return schemaElement
                    .setType(Type.BYTE_ARRAY)
                    .setLogicalType(LogicalType.ENUM(new EnumType()));
              }
            });
      }
    };
  }

  private ParquetSchemaBuilder convertMessage(
      final ParquetSchemaBuilder builder, final Descriptors.Descriptor descriptor) {
    builder.mutateElement(
        schemaElement -> {
          return schemaElement.setNum_children(descriptor.getFields().size());
        });
    for (final Descriptors.FieldDescriptor field : descriptor.getFields()) {
      builder.addChild(convertField(field));
    }
    return builder;
  }
}
