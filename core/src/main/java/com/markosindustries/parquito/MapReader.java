package com.markosindustries.parquito;

import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.FieldVisitor;
import com.markosindustries.parquito.types.ConversionStrategy;
import com.markosindustries.parquito.types.JavaTypesConversionStrategy;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MapReader implements Reader<Map<String, Object>> {
  private final MapRowBuilder rowBuilder;

  public MapReader(final ParquetSchemaNode parquetSchemaNode) {
    this(parquetSchemaNode, new JavaTypesConversionStrategy());
  }

  public MapReader(
      final ParquetSchemaNode parquetSchemaNode, final ConversionStrategy conversionStrategy) {
    this.rowBuilder = new MapRowBuilder(parquetSchemaNode, conversionStrategy);
  }

  public Reader.RowBuilder<Map<String, Object>> rowBuilder() {
    return rowBuilder;
  }

  static class MapRowBuilder extends MapRowBranchBuilder
      implements RowBuilder<Map<String, Object>> {
    public MapRowBuilder(
        final ParquetSchemaNode parquetSchemaNode, final ConversionStrategy conversionStrategy) {
      super(parquetSchemaNode, conversionStrategy, null);
    }

    @Override
    public void endBranch() {}

    @Override
    public Map<String, Object> build() {
      final var row = map;
      map = new HashMap<>();
      return row;
    }
  }

  static class MapRowBranchBuilder implements FieldVisitor {
    protected final ParquetSchemaNode parquetSchemaNode;
    protected final LogicalTypeConverter<?> converter;
    protected final MapRowBranchBuilder parent;
    protected final FieldVisitor[] childVisitorsByFieldIndex;

    protected Map<String, Object> map;

    public MapRowBranchBuilder(
        final ParquetSchemaNode parquetSchemaNode,
        final ConversionStrategy conversionStrategy,
        final MapRowBranchBuilder parent) {
      this.parquetSchemaNode = parquetSchemaNode;
      this.converter = conversionStrategy.converterFor(parquetSchemaNode);
      this.parent = parent;
      this.childVisitorsByFieldIndex = new FieldVisitor[parquetSchemaNode.getChildren().length];
      for (var i = 0; i < this.childVisitorsByFieldIndex.length; i++) {
        final var childAtIndex = parquetSchemaNode.getChildAtIndex(i);

        switch (childAtIndex.getRepetitionType()) {
          case REQUIRED, OPTIONAL -> {
            childVisitorsByFieldIndex[i] =
                new MapRowBranchBuilder(childAtIndex, conversionStrategy, this);
          }
          case REPEATED -> {
            childVisitorsByFieldIndex[i] =
                new MapRowRepeatedBuilder(childAtIndex, conversionStrategy, this);
          }
        }
      }

      map = new HashMap<>(parquetSchemaNode.getChildren().length);
    }

    @Override
    public FieldVisitor forChildIndex(final int childIndex) {
      return childVisitorsByFieldIndex[childIndex];
    }

    @Override
    public void endBranch() {
      parent.map.put(parquetSchemaNode.getElement().name, map);
      map = new HashMap<>(parquetSchemaNode.getChildren().length);
    }

    @Override
    public void endRepeated() {}

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      parent.map.put(parquetSchemaNode.getElement().name, converter.from(values, valueIndex));
    }

    @Override
    public void visitNull(final int pageIndex) {}
  }

  static class MapRowRepeatedBuilder extends MapRowBranchBuilder implements FieldVisitor {
    private ArrayList<Object> list;

    public MapRowRepeatedBuilder(
        final ParquetSchemaNode parquetSchemaNode,
        final ConversionStrategy conversionStrategy,
        final MapRowBranchBuilder parent) {
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
      parent.map.put(parquetSchemaNode.getElement().name, list);
      list = new ArrayList<>();
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      list.add(converter.from(values, valueIndex));
    }

    @Override
    public void visitNull(final int pageIndex) {
      list.add(null);
    }
  }
}
