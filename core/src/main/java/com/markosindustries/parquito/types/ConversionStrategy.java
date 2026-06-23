package com.markosindustries.parquito.types;

import com.markosindustries.parquito.ParquetSchemaNode;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.Type;

public interface ConversionStrategy {
  ConversionStrategy DEFAULT = new JavaTypesConversionStrategy();
  ConversionStrategy IDENTITY = new IdentityConversionStrategy();

  default LogicalTypeConverter<?> converterFor(final ParquetSchemaNode schemaNode) {
    return converterFor(
        schemaNode.getElement().type,
        schemaNode.getElement().logicalType,
        schemaNode.getElement().type_length);
  }

  LogicalTypeConverter<?> converterFor(
      final Type type, final LogicalType logicalType, final int typeLength);
}
