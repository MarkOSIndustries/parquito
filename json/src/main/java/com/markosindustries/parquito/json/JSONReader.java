package com.markosindustries.parquito.json;

import com.alibaba.fastjson2.JSONObject;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.json.types.JSONTypeConversionStrategy;
import com.markosindustries.parquito.rows.FieldVisitor;
import com.markosindustries.parquito.types.ConversionStrategy;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JSONReader implements Reader<JSONObject> {
  private final JSONRowBuilder rowBuilder;

  public JSONReader(final ParquetSchemaNode parquetSchemaNode) {
    this(parquetSchemaNode, new JSONTypeConversionStrategy());
  }

  public JSONReader(
      final ParquetSchemaNode parquetSchemaNode, final ConversionStrategy conversionStrategy) {
    this.rowBuilder = new JSONRowBuilder(parquetSchemaNode, conversionStrategy);
  }

  @Override
  public Reader.RowBuilder<JSONObject> rowBuilder() {
    return rowBuilder;
  }

  static class JSONRowBuilder extends JSONRowBranchBuilder implements RowBuilder<JSONObject> {
    public JSONRowBuilder(
        final ParquetSchemaNode parquetSchemaNode, final ConversionStrategy conversionStrategy) {
      super(parquetSchemaNode, conversionStrategy, null);
    }

    @Override
    public void endBranch() {}

    @Override
    public JSONObject build() {
      final var row = jsonObject;
      jsonObject = new JSONObject(parquetSchemaNode.getChildren().length);
      return row;
    }
  }

  static class JSONRowBranchBuilder implements FieldVisitor {
    protected final ParquetSchemaNode parquetSchemaNode;
    protected final LogicalTypeConverter<?> converter;
    protected final JSONRowBranchBuilder parent;
    protected final FieldVisitor[] childVisitorsByFieldIndex;
    protected JSONObject jsonObject;

    public JSONRowBranchBuilder(
        final ParquetSchemaNode parquetSchemaNode,
        final ConversionStrategy conversionStrategy,
        final JSONRowBranchBuilder parent) {
      this.parquetSchemaNode = parquetSchemaNode;
      this.converter = conversionStrategy.converterFor(parquetSchemaNode);
      this.parent = parent;

      this.childVisitorsByFieldIndex = new FieldVisitor[parquetSchemaNode.getChildren().length];
      for (var i = 0; i < this.childVisitorsByFieldIndex.length; i++) {
        final var childAtIndex = parquetSchemaNode.getChildAtIndex(i);

        switch (childAtIndex.getRepetitionType()) {
          case REQUIRED, OPTIONAL -> {
            childVisitorsByFieldIndex[i] =
                new JSONRowBranchBuilder(childAtIndex, conversionStrategy, this);
          }
          case REPEATED -> {
            childVisitorsByFieldIndex[i] =
                new JSONRowRepeatedBuilder(childAtIndex, conversionStrategy, this);
          }
        }
      }

      this.jsonObject = new JSONObject(parquetSchemaNode.getChildren().length);
    }

    @Override
    public FieldVisitor forChildIndex(final int childIndex) {
      return childVisitorsByFieldIndex[childIndex];
    }

    @Override
    public void endBranch() {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, jsonObject);
      jsonObject = new JSONObject(parquetSchemaNode.getChildren().length);
    }

    @Override
    public void endRepeated() {}

    @Override
    public void visit(final int pageIndex, final boolean value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromBoolean(value));
    }

    @Override
    public void visit(final int pageIndex, final ByteBuffer value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromByteBuffer(value));
    }

    @Override
    public void visit(final int pageIndex, final float value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromFloat(value));
    }

    @Override
    public void visit(final int pageIndex, final double value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromDouble(value));
    }

    @Override
    public void visit(final int pageIndex, final int value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromInt32(value));
    }

    @Override
    public void visit(final int pageIndex, final long value) {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, converter.fromInt64(value));
    }

    @Override
    public void visitNull(final int pageIndex) {}
  }

  static class JSONRowRepeatedBuilder extends JSONRowBranchBuilder implements FieldVisitor {
    private ArrayList<Object> list;
    private Map<String, Object> map;

    public JSONRowRepeatedBuilder(
        final ParquetSchemaNode parquetSchemaNode,
        final ConversionStrategy conversionStrategy,
        final JSONRowBranchBuilder parent) {
      super(parquetSchemaNode, conversionStrategy, parent);

      list = new ArrayList<>();
    }

    @Override
    public FieldVisitor forChildIndex(final int childIndex) {
      return childVisitorsByFieldIndex[childIndex];
    }

    @Override
    public void endBranch() {
      list.add(map);
      map = new HashMap<>(parquetSchemaNode.getChildren().length);
    }

    @Override
    public void endRepeated() {
      parent.jsonObject.put(parquetSchemaNode.getElement().name, list);
      list = new ArrayList<>();
    }

    @Override
    public void visit(final int pageIndex, final boolean value) {
      list.add(converter.fromBoolean(value));
    }

    @Override
    public void visit(final int pageIndex, final ByteBuffer value) {
      list.add(converter.fromByteBuffer(value));
    }

    @Override
    public void visit(final int pageIndex, final float value) {
      list.add(converter.fromFloat(value));
    }

    @Override
    public void visit(final int pageIndex, final double value) {
      list.add(converter.fromDouble(value));
    }

    @Override
    public void visit(final int pageIndex, final int value) {
      list.add(converter.fromInt32(value));
    }

    @Override
    public void visit(final int pageIndex, final long value) {
      list.add(converter.fromInt64(value));
    }

    @Override
    public void visitNull(final int pageIndex) {
      list.add(null);
    }
  }
}
