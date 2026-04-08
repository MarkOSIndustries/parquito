package com.markosindustries.parquito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaNode {
  private final ParquetSchemaNode parent;
  private final SchemaElement element;
  private final Map<String, ParquetSchemaNode> childrenByName;
  private final SparseArrayIndexMap<ParquetSchemaNode> childrenByFieldId;
  private final Set<Integer> childFieldIds;
  private final int nodeCount;
  private final int repetitionLevelMax;
  private final int definitionLevelMax;

  /** Just a type-system way to specify whether we want the root, or any node will do */
  public static class Root extends ParquetSchemaNode {
    private Root(
        final SchemaElement element,
        final List<SchemaElement> remainder,
        final int repetitionLevelMax,
        final int definitionLevelMax) {
      super(null, element, remainder, repetitionLevelMax, definitionLevelMax);
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
  }

  public static Root from(final List<SchemaElement> schema) {
    if (schema.isEmpty()) {
      throw new IllegalArgumentException("Can't create a schema with no elements");
    }
    return new Root(schema.get(0), schema.subList(1, schema.size()), 0, 0);
  }

  protected ParquetSchemaNode(
      ParquetSchemaNode parent,
      SchemaElement element,
      List<SchemaElement> remainder,
      int repetitionLevelMax,
      int definitionLevelMax) {
    this.parent = parent;
    this.element = element;

    if (!element.isSetRepetition_type()) {
      this.repetitionLevelMax = repetitionLevelMax;
      this.definitionLevelMax = definitionLevelMax;
    } else {
      switch (element.repetition_type) {
        case REQUIRED -> {
          this.repetitionLevelMax = repetitionLevelMax;
          this.definitionLevelMax = definitionLevelMax;
        }
        case OPTIONAL -> {
          this.repetitionLevelMax = repetitionLevelMax;
          this.definitionLevelMax = definitionLevelMax + 1;
        }
        case REPEATED -> {
          this.repetitionLevelMax = repetitionLevelMax + 1;
          this.definitionLevelMax = definitionLevelMax + 1;
        }
        default ->
            throw new IllegalArgumentException(
                "Unsupported repetition_type: " + element.repetition_type);
      }
    }

    final var children = new ArrayList<ParquetSchemaNode>(element.num_children);
    var remaining = remainder;
    for (int i = 0; i < element.num_children; i++) {
      final var nextChild =
          new ParquetSchemaNode(
              this,
              remaining.getFirst(),
              remaining.subList(1, remaining.size()),
              this.repetitionLevelMax,
              this.definitionLevelMax);
      remaining = remaining.subList(nextChild.nodeCount, remaining.size());
      children.add(nextChild);
    }
    this.childrenByName =
        children.stream()
            .collect(Collectors.toUnmodifiableMap(c -> c.element.name, Function.identity()));
    this.childrenByFieldId =
        SparseArrayIndexMap.from(children, c -> c.element.field_id, ParquetSchemaNode[]::new);
    this.childFieldIds =
        children.stream().map(c -> c.element.field_id).collect(Collectors.toUnmodifiableSet());
    this.nodeCount = 1 + children.stream().mapToInt(child -> child.nodeCount).sum();
  }

  public SchemaElement getElement() {
    return element;
  }

  public ParquetSchemaPath getPath() {
    final var path = new ArrayList<SchemaElement>();
    ParquetSchemaNode current = this;
    while (!(current instanceof Root)) {
      path.addFirst(current.element);
      current = current.parent;
    }
    return new ParquetSchemaPath(path.toArray(SchemaElement[]::new));
  }

  public ParquetSchemaNode getChildByName(String name) {
    return this.childrenByName.get(name);
  }

  public ParquetSchemaNode getChild(int childFieldId) {
    return this.childrenByFieldId.get(childFieldId);
  }

  public ParquetSchemaNode getChild(final ParquetSchemaPath parquetSchemaPath) {
    var current = this;
    for (final var element : parquetSchemaPath.path) {
      current = current.childrenByFieldId.get(element.field_id);
    }
    return current;
  }

  public Set<Integer> getChildFieldIds() {
    return childFieldIds;
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

  //
  //  public <ReadAs> RequiredColumnAccessor<ReadAs>
  // getRequiredColumnAccessor(String... schemaPath) {
  //    return new RequiredColumnAccessor<>(getChild(schemaPath).element);
  //  }
  //
  //  public <ReadAs> OptionalColumnAccessor<ReadAs>
  // getOptionalColumnAccessor(String... schemaPath) {
  //    return new OptionalColumnAccessor<>(getChild(schemaPath).element);
  //  }
  //
  //  public <ReadAs> RepeatedColumnAccessor<ReadAs>
  // getRepeatedColumnAccessor(String... schemaPath) {
  //    return new RepeatedColumnAccessor<>(getChild(schemaPath).element);
  //  }

  @Override
  public String toString() {
    return "ParquetSchema{"
        + "element="
        + element
        + ", childrenByFieldId="
        + childrenByFieldId
            .valuesStream()
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
