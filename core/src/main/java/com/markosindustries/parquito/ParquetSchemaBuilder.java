package com.markosindustries.parquito;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.parquet.format.SchemaElement;

public class ParquetSchemaBuilder {
  private SchemaElement element;
  private final List<ParquetSchemaBuilder> children;

  public ParquetSchemaBuilder(String name) {
    this(new SchemaElement(name));
  }

  public ParquetSchemaBuilder(SchemaElement element) {
    this.element = element;
    this.children = new ArrayList<>();
  }

  public ParquetSchemaBuilder mutateElement(Function<SchemaElement, SchemaElement> mutator) {
    element = mutator.apply(element);
    return this;
  }

  public ParquetSchemaBuilder addChild(ParquetSchemaBuilder child) {
    children.add(child);
    return this;
  }

  public List<SchemaElement> build() {
    element.setNum_children(children.size());
    return new ArrayList<>() {
      {
        add(element);
        if (!children.isEmpty()) {
          addAll(children.stream().flatMap(c -> c.build().stream()).toList());
        }
      }
    };
  }
}
