package com.markosindustries.parquito;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaPath {
  final int[] pathAsFieldIndices;
  final SchemaElement[] path;

  ParquetSchemaPath(final int[] pathAsFieldIndices, final SchemaElement[] path) {
    this.pathAsFieldIndices = pathAsFieldIndices;
    this.path = path;
  }

  public int getPathLength() {
    return pathAsFieldIndices.length;
  }

  public int getFieldIndexDepth(int depth) {
    return pathAsFieldIndices[depth];
  }

  private static final ParquetSchemaPath EMPTY =
      new ParquetSchemaPath(new int[0], new SchemaElement[0]);

  static ParquetSchemaPath empty() {
    return EMPTY;
  }

  ParquetSchemaPath child(final int fieldIndex, final SchemaElement element) {
    final var child =
        new ParquetSchemaPath(
            new int[this.pathAsFieldIndices.length + 1], new SchemaElement[this.path.length + 1]);

    System.arraycopy(
        this.pathAsFieldIndices, 0, child.pathAsFieldIndices, 0, this.pathAsFieldIndices.length);
    child.pathAsFieldIndices[this.pathAsFieldIndices.length] = fieldIndex;

    System.arraycopy(this.path, 0, child.path, 0, this.path.length);
    child.path[this.path.length] = element;

    return child;
  }

  List<String> asNamesOnly() {
    return Arrays.stream(path).map(SchemaElement::getName).toList();
  }

  @Override
  public String toString() {
    return Arrays.stream(path).map(SchemaElement::getName).collect(Collectors.joining("."));
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    final ParquetSchemaPath that = (ParquetSchemaPath) o;
    return Objects.deepEquals(pathAsFieldIndices, that.pathAsFieldIndices);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(pathAsFieldIndices);
  }

  static ParquetSchemaPath parsePathElements(
      final ParquetSchemaNode.Root schema, final List<String> path) {
    final int[] pathAsFieldIndices = new int[path.size()];
    final SchemaElement[] pathSchemaElements = new SchemaElement[path.size()];
    ParquetSchemaNode currentSchemaNode = schema;
    for (var i = 0; i < path.size(); i++) {
      final var fieldName = path.get(i);
      final var nextIndex =
          currentSchemaNode
              .findIndexOfChildByName(fieldName)
              .orElseThrow(
                  () ->
                      new IndexOutOfBoundsException(
                          "Field "
                              + fieldName
                              + " does not exist (in given path "
                              + String.join(".", path)
                              + ")"));
      final var nextSchemaNode = currentSchemaNode.getChildAtIndex(nextIndex);
      pathAsFieldIndices[i] = nextIndex;
      pathSchemaElements[i] = nextSchemaNode.getElement();
      currentSchemaNode = nextSchemaNode;
    }
    return new ParquetSchemaPath(pathAsFieldIndices, pathSchemaElements);
  }

  static ParquetSchemaPath parsePathElements(ParquetSchemaNode.Root schema, String... path) {
    final int[] pathAsFieldIndices = new int[path.length];
    final SchemaElement[] pathSchemaElements = new SchemaElement[path.length];
    ParquetSchemaNode currentSchemaNode = schema;
    for (var i = 0; i < path.length; i++) {
      final var fieldName = path[i];
      final var nextIndex =
          currentSchemaNode
              .findIndexOfChildByName(fieldName)
              .orElseThrow(
                  () ->
                      new IndexOutOfBoundsException(
                          "Field "
                              + fieldName
                              + " does not exist (in given path "
                              + String.join(".", path)
                              + ")"));
      final var nextSchemaNode = currentSchemaNode.getChildAtIndex(nextIndex);
      pathAsFieldIndices[i] = nextIndex;
      pathSchemaElements[i] = nextSchemaNode.getElement();
      currentSchemaNode = nextSchemaNode;
    }
    return new ParquetSchemaPath(pathAsFieldIndices, pathSchemaElements);
  }

  static ParquetSchemaPath parseDotSeparatedPath(
      ParquetSchemaNode.Root schema, String dotSeparatedPath) {
    return parsePathElements(schema, dotSeparatedPath.split("\\."));
  }
}
