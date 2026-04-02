package com.markosindustries.parquito;

public interface ParquetSchemaPath {
  static String[] parse(String dotSeparatedPath) {
    return dotSeparatedPath.split("\\.");
  }
}
