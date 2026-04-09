package com.markosindustries.parquito;

import java.util.List;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaPath {
  final int[] pathAsFieldIndices;
  final SchemaElement[] path;

  ParquetSchemaPath(final int[] pathAsFieldIndices, final SchemaElement[] path) {
    this.pathAsFieldIndices = pathAsFieldIndices;
    this.path = path;
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
