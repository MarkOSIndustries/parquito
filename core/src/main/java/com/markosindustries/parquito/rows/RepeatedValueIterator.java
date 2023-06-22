package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.page.DataPage;
import java.util.Iterator;

public class RepeatedValueIterator<ReadAs extends Comparable<ReadAs>, Repeated, Value>
    implements ParquetFieldIterator<Repeated> {
  private final Iterator<DataPage<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final Reader<Repeated, Value> reader;
  private DataPage<ReadAs> dataPage = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public RepeatedValueIterator(
      Iterator<DataPage<ReadAs>> dataPageIterator,
      ParquetSchemaNode schemaNode,
      Reader<Repeated, Value> reader) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    this.reader = reader;
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
  public Repeated next() {
    final var values = reader.repeatedBuilder();
    if (dataPage.getDefinitionLevels()[definitionIndex] == schemaNode.getDefinitionLevelMax()) {
      do {
        //noinspection unchecked
        values.add((Value) dataPage.getValue(valueIndex++));
        definitionIndex++;
        advancePageIfNecessary();
      } while (dataPage != null
          && dataPage.getRepetitionLevels()[definitionIndex] == schemaNode.getRepetitionLevelMax());
    } else {
      definitionIndex++;
      advancePageIfNecessary();
    }

    return values.build();
  }
}
