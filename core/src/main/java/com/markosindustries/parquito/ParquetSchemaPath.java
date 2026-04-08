package com.markosindustries.parquito;

import java.util.List;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaPath {
  final SchemaElement[] path;

  ParquetSchemaPath(SchemaElement[] path) {
    this.path = path;
  }

  static ParquetSchemaPath parsePathElements(ParquetSchemaNode.Root schema, List<String> path) {
    final SchemaElement[] pathSchemaElements = new SchemaElement[path.size()];
    ParquetSchemaNode currentSchemaNode = schema;
    for (var i = 0; i < path.size(); i++) {
      final var nextSchemaNode = currentSchemaNode.getChildByName(path.get(i));
      pathSchemaElements[i] = nextSchemaNode.getElement();
      currentSchemaNode = nextSchemaNode;
    }
    return new ParquetSchemaPath(pathSchemaElements);
  }

  static ParquetSchemaPath parsePathElements(ParquetSchemaNode.Root schema, String... path) {
    final SchemaElement[] pathSchemaElements = new SchemaElement[path.length];
    ParquetSchemaNode currentSchemaNode = schema;
    for (var i = 0; i < path.length; i++) {
      final var nextSchemaNode = currentSchemaNode.getChildByName(path[i]);
      pathSchemaElements[i] = nextSchemaNode.getElement();
      currentSchemaNode = nextSchemaNode;
    }
    return new ParquetSchemaPath(pathSchemaElements);
  }

  static ParquetSchemaPath parseDotSeparatedPath(
      ParquetSchemaNode.Root schema, String dotSeparatedPath) {
    return parsePathElements(schema, dotSeparatedPath.split("\\."));
  }
}
