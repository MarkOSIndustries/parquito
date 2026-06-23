package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.FieldVisitor;

public interface Reader<Row> {
  RowBuilder<Row> rowBuilder();

  interface RowBuilder<Row> extends FieldVisitor {
    Row build();
  }
}
