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
        default -> throw new IllegalArgumentException(
            "Unsupported repetition_type: " + element.repetition_type);
      }
    }

    final var children = new ArrayList<ParquetSchemaNode>(element.num_children);
    var remaining = remainder;
    for (int i = 0; i < element.num_children; i++) {
      final var nextChild =
          new ParquetSchemaNode(
              this,
              remaining.get(0),
              remaining.subList(1, remaining.size()),
              this.repetitionLevelMax,
              this.definitionLevelMax);
      remaining = remaining.subList(nextChild.nodeCount, remaining.size());
      children.add(nextChild);
    }
    this.childrenByName =
        children.stream()
            .collect(Collectors.toUnmodifiableMap(c -> c.element.name, Function.identity()));
    this.nodeCount = 1 + children.stream().mapToInt(child -> child.nodeCount).sum();
  }

  public SchemaElement getElement() {
    return element;
  }

  public String[] getPath() {
    final var path = new ArrayList<>();
    ParquetSchemaNode current = this;
    while (!(current instanceof Root)) {
      path.add(0, current.element.name);
      current = current.parent;
    }
    return path.toArray(String[]::new);
  }

  public ParquetSchemaNode getChild(Iterable<String> schemaPath) {
    var current = this;
    for (final String name : schemaPath) {
      current = current.childrenByName.get(name);
    }
    return current;
  }

  public ParquetSchemaNode getChild(String... schemaPath) {
    var current = this;
    for (final String name : schemaPath) {
      current = current.childrenByName.get(name);
    }
    return current;
  }

  public Set<String> getChildren() {
    return childrenByName.keySet();
  }

  public int getRepetitionLevelMax() {
    return repetitionLevelMax;
  }

  public int getDefinitionLevelMax() {
    return definitionLevelMax;
  }

  public FieldRepetitionType getRepetitionType(String... schemaPath) {
    return getChild(schemaPath).element.repetition_type;
  }

  public ConvertedType getConvertedType(String... schemaPath) {
    return getChild(schemaPath).element.converted_type;
  }

  public LogicalType getLogicalType(String... schemaPath) {
    return getChild(schemaPath).element.logicalType;
  }

  public int getTypeLength(String... schemaPath) {
    return getChild(schemaPath).element.type_length;
  }
  //
  //  public <ReadAs extends Comparable<ReadAs>> RequiredColumnAccessor<ReadAs>
  // getRequiredColumnAccessor(String... schemaPath) {
  //    return new RequiredColumnAccessor<>(getChild(schemaPath).element);
  //  }
  //
  //  public <ReadAs extends Comparable<ReadAs>> OptionalColumnAccessor<ReadAs>
  // getOptionalColumnAccessor(String... schemaPath) {
  //    return new OptionalColumnAccessor<>(getChild(schemaPath).element);
  //  }
  //
  //  public <ReadAs extends Comparable<ReadAs>> RepeatedColumnAccessor<ReadAs>
  // getRepeatedColumnAccessor(String... schemaPath) {
  //    return new RepeatedColumnAccessor<>(getChild(schemaPath).element);
  //  }

  @Override
  public String toString() {
    return "ParquetSchema{"
        + "element="
        + element
        + ", childrenByName="
        + childrenByName.values().stream()
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
