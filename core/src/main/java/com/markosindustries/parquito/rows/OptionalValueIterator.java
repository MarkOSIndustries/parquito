package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;
import com.markosindustries.parquito.page.DataPage;
import java.util.Iterator;

public class OptionalValueIterator<ReadAs, Value> implements ParquetFieldIterator<ReadAs> {
  private final Iterator<DataPage<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final RowReadSpec<?, Value> rowReadSpec;
  private DataPage<ReadAs> dataPage = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public OptionalValueIterator(
      Iterator<DataPage<ReadAs>> dataPageIterator,
      ParquetSchemaNode schemaNode,
      RowReadSpec<?, Value> rowReadSpec) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    this.rowReadSpec = rowReadSpec;
    if (dataPageIterator.hasNext()) {
      this.dataPage = dataPageIterator.next();
    }
  }

  @Override
  public int peekDefinitionLevel() {
    return dataPage.getDefinitionLevels()[definitionIndex];
  }

  @Override
  public int peekRepetitionLevel() {
    return dataPage.getRepetitionLevels()[definitionIndex];
  }

  @Override
  public boolean hasNext() {
    return dataPage != null;
  }

  private void advancePageIfNecessary() {
    if (definitionIndex == dataPage.getDefinitionLevels().length) {
      if (dataPageIterator.hasNext()) {
        dataPage = dataPageIterator.next();
      } else {
        dataPage = null;
      }
      definitionIndex = 0;
      valueIndex = 0;
    }
  }

  @Override
  public ReadAs next() {
    final var result =
        dataPage.getDefinitionLevels()[definitionIndex++] == schemaNode.getDefinitionLevelMax()
            ? dataPage.getValue(valueIndex++)
            : null;

    advancePageIfNecessary();

    return result;
  }
}
