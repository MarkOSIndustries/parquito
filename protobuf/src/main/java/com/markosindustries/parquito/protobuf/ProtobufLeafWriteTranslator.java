package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import java.nio.ByteBuffer;

public interface ProtobufLeafWriteTranslator<Value, WriteAs>
    extends WriteTranslator<Value, WriteAs> {
  static ProtobufLeafWriteTranslator<?, ?> forType(
      final Descriptors.FieldDescriptor fieldDescriptor,
      final ParquetSchemaNode parquetSchemaNode) {
    return switch (fieldDescriptor.getType()) {
      case DOUBLE,
              FLOAT,
              INT64,
              UINT64,
              INT32,
              FIXED64,
              FIXED32,
              BOOL,
              STRING,
              UINT32,
              SFIXED32,
              SFIXED64,
              SINT32,
              SINT64 ->
          new ProtobufSimpleLeafWriteTranslator<>();
      case BYTES -> new ProtobufBytesLeafWriteTranslator();
      case ENUM ->
          switch (parquetSchemaNode.getElement().getType()) {
            case INT32 -> ProtobufEnumAsInt32LeafWriteTranslator.INSTANCE;
            case BYTE_ARRAY -> ProtobufEnumAsStringLeafWriteTranslator.INSTANCE;
            default ->
                throw new UnsupportedOperationException(
                    "Something probably went wrong in schema conversion - protobuf enums can only be written as int32 or string, but we got "
                        + parquetSchemaNode.getElement());
          };
      case MESSAGE, GROUP ->
          throw new IllegalArgumentException(
              "We shouldn't be translating a message or group in a leaf - "
                  + parquetSchemaNode.getElement());
    };
  }

  @Override
  default Object getField(final int childIndex, final Value value) {
    throw new UnsupportedOperationException("Leaf nodes don't have children");
  }

  @Override
  default WriteTranslator<?, ?> forChildIndex(final int childIndex) {
    throw new UnsupportedOperationException("Leaf nodes don't have children");
  }

  class ProtobufSimpleLeafWriteTranslator<Value>
      implements ProtobufLeafWriteTranslator<Value, Value> {
    @Override
    public Value translate(final Value value) {
      return value;
    }
  }

  class ProtobufEnumAsInt32LeafWriteTranslator
      implements ProtobufLeafWriteTranslator<Descriptors.EnumValueDescriptor, Integer> {
    public static final ProtobufEnumAsInt32LeafWriteTranslator INSTANCE =
        new ProtobufEnumAsInt32LeafWriteTranslator();

    @Override
    public Integer translate(final Descriptors.EnumValueDescriptor value) {
      return value.getNumber();
    }
  }

  class ProtobufEnumAsStringLeafWriteTranslator
      implements ProtobufLeafWriteTranslator<Descriptors.EnumValueDescriptor, String> {
    public static final ProtobufEnumAsStringLeafWriteTranslator INSTANCE =
        new ProtobufEnumAsStringLeafWriteTranslator();

    @Override
    public String translate(final Descriptors.EnumValueDescriptor value) {
      return value.getName();
    }
  }

  class ProtobufBytesLeafWriteTranslator
      implements ProtobufLeafWriteTranslator<ByteString, ByteBuffer> {
    public static final ProtobufBytesLeafWriteTranslator INSTANCE =
        new ProtobufBytesLeafWriteTranslator();

    @Override
    public ByteBuffer translate(final ByteString bytes) {
      return bytes.asReadOnlyByteBuffer();
    }
  }
}
