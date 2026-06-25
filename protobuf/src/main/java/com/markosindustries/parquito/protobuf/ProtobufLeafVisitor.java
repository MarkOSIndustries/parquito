package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.FieldVisitor;
import java.util.function.Consumer;

public abstract sealed class ProtobufLeafVisitor implements FieldVisitor
    permits ProtobufLeafVisitor.Bools,
        ProtobufLeafVisitor.Doubles,
        ProtobufLeafVisitor.Floats,
        ProtobufLeafVisitor.Int32s,
        ProtobufLeafVisitor.Int64s,
        ProtobufLeafVisitor.Bytes,
        ProtobufLeafVisitor.Strings,
        ProtobufLeafVisitor.EnumsAsStrings,
        ProtobufLeafVisitor.EnumsAsInt32s {
  protected final Consumer<Object> storeValueInParent;

  protected ProtobufLeafVisitor(final Consumer<Object> storeValueInParent) {
    this.storeValueInParent = storeValueInParent;
  }

  static ProtobufLeafVisitor create(
      final ParquetSchemaNode parquetSchemaNode,
      final Descriptors.FieldDescriptor field,
      final Consumer<Object> storeValueInParent) {
    return switch (field.getType()) {
      case DOUBLE -> new Doubles(storeValueInParent);
      case FLOAT -> new Floats(storeValueInParent);
      case INT64, SINT64, FIXED64, SFIXED64, UINT64 -> new Int64s(storeValueInParent);
      case INT32, SINT32, FIXED32, SFIXED32, UINT32 -> new Int32s(storeValueInParent);
      case BOOL -> new Bools(storeValueInParent);
      case STRING -> new Strings(storeValueInParent);
      case GROUP ->
          throw new UnsupportedOperationException(
              "Encountered a protobuf Group in a leaf schema node");
      case MESSAGE ->
          throw new UnsupportedOperationException(
              "Encountered a protobuf Message in a leaf schema node");
      case BYTES -> new Bytes(storeValueInParent);
      case ENUM ->
          switch (parquetSchemaNode.getElement().type) {
            case INT32 -> new EnumsAsInt32s(field, storeValueInParent);
            case BYTE_ARRAY -> new EnumsAsStrings(field, storeValueInParent);
            default ->
                throw new UnsupportedOperationException(
                    "Reading protobuf enum from a "
                        + parquetSchemaNode.getElement().type
                        + " is not supported");
          };
    };
  }

  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    throw new UnsupportedOperationException("Leaf nodes don't have children");
  }

  @Override
  public void endBranch() {
    throw new UnsupportedOperationException("Unexpected end of branch in leaf node");
  }

  @Override
  public void endRepeated() {}

  @Override
  public void visitNull(final int pageIndex) {}

  static final class Bools extends ProtobufLeafVisitor {
    Bools(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(values.getBoolean(valueIndex));
    }
  }

  static final class Doubles extends ProtobufLeafVisitor {
    Doubles(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(values.getDouble(valueIndex));
    }
  }

  static final class Floats extends ProtobufLeafVisitor {
    Floats(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(values.getFloat(valueIndex));
    }
  }

  static final class Int32s extends ProtobufLeafVisitor {
    Int32s(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(values.getInt32(valueIndex));
    }
  }

  static final class Int64s extends ProtobufLeafVisitor {
    Int64s(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(values.getInt64(valueIndex));
    }
  }

  static final class Bytes extends ProtobufLeafVisitor {
    Bytes(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(ByteString.copyFrom((values.getByteBuffer(valueIndex))));
    }
  }

  static final class Strings extends ProtobufLeafVisitor {
    Strings(final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(
          ByteString.copyFrom((values.getByteBuffer(valueIndex))).toStringUtf8());
    }
  }

  // TODO - we can cache these (bonus points - can we pre-transform everything in a dictionary
  //   regardless of Reader impl?)
  static final class EnumsAsStrings extends ProtobufLeafVisitor {
    private final Descriptors.EnumDescriptor enumType;

    EnumsAsStrings(
        final Descriptors.FieldDescriptor field, final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
      this.enumType = field.getEnumType();
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(
          enumType.findValueByName(
              ByteString.copyFrom(values.getByteBuffer(valueIndex)).toStringUtf8()));
    }
  }

  static final class EnumsAsInt32s extends ProtobufLeafVisitor {
    private final Descriptors.EnumDescriptor enumType;

    EnumsAsInt32s(
        final Descriptors.FieldDescriptor field, final Consumer<Object> storeValueInParent) {
      super(storeValueInParent);
      this.enumType = field.getEnumType();
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      storeValueInParent.accept(enumType.findValueByNumber(values.getInt32(valueIndex)));
    }
  }
}
