package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;

public class ProtobufListWriteTranslator implements WriteTranslator<Object, Void> {
  private final ListGroupWriteTranslator<?, ?> listGroupWriteTranslator;

  public ProtobufListWriteTranslator(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode schemaNode) {
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);
    this.listGroupWriteTranslator =
        new ListGroupWriteTranslator<>(
            ProtobufMessageWriteTranslator.determineAppropriateTranslator(
                field, elementSchemaNode));
  }

  @Override
  public Object getField(final int childIndex, final Object list) {
    return list; // we want to just hand the list to the child to deal with
  }

  @Override
  public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
    return listGroupWriteTranslator;
  }

  @Override
  public Void translate(final Object o) {
    throw new UnsupportedOperationException(
        "A protobuf repeated field cannot be directly translated, as it is not a leaf node in the parquet schema");
  }

  public static class ListGroupWriteTranslator<Value, WriteAs>
      implements WriteTranslator<Value, WriteAs> {
    private final WriteTranslator<Value, WriteAs> elementTranslator;

    public ListGroupWriteTranslator(final WriteTranslator<Value, WriteAs> elementTranslator) {
      this.elementTranslator = elementTranslator;
    }

    @Override
    public Object getField(final int childIndex, final Value value) {
      return value; // just hand it back to go to the element write
    }

    @Override
    public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
      return elementTranslator;
    }

    @Override
    public WriteAs translate(final Value value) {
      throw new UnsupportedOperationException(
          "A protobuf repeated field's list group node cannot be directly translated, as it is not a leaf node in the parquet schema");
    }
  }
}
