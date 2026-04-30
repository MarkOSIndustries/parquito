package com.markosindustries.parquito;

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaNode {
  private final ParquetSchemaNode parent;
  private final ParquetSchemaPath schemaPath;
  private final SchemaElement element;
  private final OptionalInt columnIndex;
  private final ParquetSchemaNode[] children;
  private final Object2IntArrayMap<String> childIndicesByName;
  protected final int nodeCount;
  protected final int leafCount;
  private final int repetitionLevelMax;
  private final int definitionLevelMax;

  /** Just a type-system way to specify whether we want the root, or any node will do */
  public static class Root extends ParquetSchemaNode {
    private Root(
        final SchemaElement element,
        final List<SchemaElement> remainder,
        final int repetitionLevelMax,
        final int definitionLevelMax) {
      super(
          null,
          ParquetSchemaPath.empty(),
          element,
          new AtomicInteger(0),
          remainder,
          repetitionLevelMax,
          definitionLevelMax);
    }

    public ParquetSchemaPath parsePathElements(List<String> path) {
      return ParquetSchemaPath.parsePathElements(this, path);
    }

    public ParquetSchemaPath parsePathElements(String... path) {
      return ParquetSchemaPath.parsePathElements(this, path);
    }

    public ParquetSchemaPath parseDotSeparatedPath(String dotSeparatedPath) {
      return ParquetSchemaPath.parseDotSeparatedPath(this, dotSeparatedPath);
    }

    public List<ParquetSchemaNode> findLeafNodes() {
      return findLeafNodesRecursive().toList();
    }
  }

  public static Root from(final List<SchemaElement> schema) {
    if (schema.isEmpty()) {
      throw new IllegalArgumentException("Can't create a schema with no elements");
    }
    return new Root(schema.getFirst(), schema.subList(1, schema.size()), 0, 0);
  }

  protected ParquetSchemaNode(
      ParquetSchemaNode parent,
      ParquetSchemaPath schemaPath,
      SchemaElement element,
      AtomicInteger nextColumnIndex,
      List<SchemaElement> remainder,
      int repetitionLevelMax,
      int definitionLevelMax) {
    this.parent = parent;
    this.schemaPath = schemaPath;
    this.element = element;

    if (!element.isSetRepetition_type()) {
      this.repetitionLevelMax = repetitionLevelMax;
      this.definitionLevelMax = definitionLevelMax;
    } else {
      this.repetitionLevelMax =
          switch (element.repetition_type) {
            case REQUIRED, OPTIONAL -> repetitionLevelMax;
            case REPEATED -> repetitionLevelMax + 1;
          };
      this.definitionLevelMax =
          switch (element.repetition_type) {
            case REQUIRED -> definitionLevelMax;
            case OPTIONAL, REPEATED -> definitionLevelMax + 1;
          };
    }

    this.columnIndex =
        element.num_children == 0
            ? OptionalInt.of(nextColumnIndex.getAndIncrement())
            : OptionalInt.empty();

    this.children = new ParquetSchemaNode[element.num_children];
    var remaining = remainder;
    for (int i = 0; i < element.num_children; i++) {
      children[i] =
          new ParquetSchemaNode(
              this,
              schemaPath.child(i, remaining.getFirst()),
              remaining.getFirst(),
              nextColumnIndex,
              remaining.subList(1, remaining.size()),
              this.repetitionLevelMax,
              this.definitionLevelMax);
      remaining = remaining.subList(children[i].nodeCount, remaining.size());
    }
    this.childIndicesByName = new Object2IntArrayMap<>(this.children.length);
    this.childIndicesByName.defaultReturnValue(-1);
    for (var i = 0; i < this.children.length; i++) {
      childIndicesByName.put(this.children[i].element.name, i);
    }
    this.nodeCount = 1 + Arrays.stream(children).mapToInt(child -> child.nodeCount).sum();
    this.leafCount =
        children.length == 0 ? 1 : Arrays.stream(children).mapToInt(child -> child.leafCount).sum();
  }

  public ParquetSchemaNode getParent() {
    return parent;
  }

  public SchemaElement getElement() {
    return element;
  }

  public ParquetSchemaPath getPath() {
    return schemaPath;
  }

  public OptionalInt getColumnIndex() {
    return columnIndex;
  }

  public ParquetSchemaNode[] getChildren() {
    return this.children;
  }

  public ParquetSchemaNode getChildAtIndex(int childFieldIndex) {
    return this.children[childFieldIndex];
  }

  public OptionalInt findIndexOfChildByName(final String name) {
    final var valueOrDefault = this.childIndicesByName.getOrDefault(name, -1);
    if (valueOrDefault == -1) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(valueOrDefault);
  }

  public ParquetSchemaNode getChild(final ParquetSchemaPath parquetSchemaPath) {
    var current = this;
    for (final var index : parquetSchemaPath.pathAsFieldIndices) {
      current = current.children[index];
    }
    return current;
  }

  public int getNodeCount() {
    return nodeCount;
  }

  public int getLeafCount() {
    return leafCount;
  }

  public int getRepetitionLevelMax() {
    return repetitionLevelMax;
  }

  public int getDefinitionLevelMax() {
    return definitionLevelMax;
  }

  public FieldRepetitionType getRepetitionType() {
    return element.repetition_type;
  }

  public ConvertedType getConvertedType() {
    return element.converted_type;
  }

  public LogicalType getLogicalType() {
    return element.logicalType;
  }

  public int getTypeLength() {
    return element.type_length;
  }

  protected Stream<ParquetSchemaNode> findLeafNodesRecursive() {
    if (children.length > 0) {
      return Arrays.stream(children).flatMap(ParquetSchemaNode::findLeafNodesRecursive);
    } else {
      return Stream.of(this);
    }
  }

  @Override
  public String toString() {
    return "ParquetSchema{"
        + "element="
        + element
        + ", children="
        + Arrays.stream(children)
            .map(ParquetSchemaNode::toString)
            .collect(Collectors.joining(", ", "[", "]"))
        + ", nodeCount="
        + nodeCount
        + ", repetitionLevelMax="
        + repetitionLevelMax
        + ", definitionLevelMax="
        + definitionLevelMax
        + '}';
  }
}
