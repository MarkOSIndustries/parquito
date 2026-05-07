package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.DataPageReader;
import java.util.Iterator;

public class OptionalValueIterator<ReadAs, Value>
    implements ParquetFieldIterator<ReadAs>, DataPageCursor<ReadAs> {
  private final EOFDataPage<ReadAs> eofDataPage = new EOFDataPage<>();

  private final Iterator<DataPageReader<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final PushdownPredicates pushdownPredicates;

  private DataPageReader<ReadAs> dataPage = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public OptionalValueIterator(
      Iterator<DataPageReader<ReadAs>> dataPageIterator,
      ParquetSchemaNode schemaNode,
      PushdownPredicates pushdownPredicates) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    this.pushdownPredicates = pushdownPredicates;
    advancePageIfNecessary();
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
  public DataPageReader<ReadAs> getDataPage() {
    return dataPage;
  }

  @Override
  public ParquetSchemaNode getSchemaNode() {
    return schemaNode;
  }

  @Override
  public int getDefinitionIndex() {
    return definitionIndex;
  }

  @Override
  public int getValueIndex() {
    return valueIndex;
  }

  @Override
  public boolean hasNext() {
    return dataPage != eofDataPage;
  }

  private void advancePageIfNecessary() {
    if (dataPage == null || definitionIndex == dataPage.getDefinitionLevels().length) {
      if (dataPageIterator.hasNext()) {
        dataPage = dataPageIterator.next();
        pushdownPredicates.newPage(this);
      } else {
        dataPage = eofDataPage;
      }
      definitionIndex = 0;
      valueIndex = 0;
    }
  }

  @Override
  public void skipNextRow() {
    do {
      if (dataPage.getDefinitionLevels()[definitionIndex] == schemaNode.getDefinitionLevelMax()) {
        valueIndex++;
      }
      definitionIndex++;
    } while (definitionIndex < dataPage.getDefinitionLevels().length
        && dataPage.getRepetitionLevels()[definitionIndex] != 0);
    advancePageIfNecessary();
  }

  @Override
  public ReadAs next() {
    final var result =
        dataPage.getDefinitionLevels()[definitionIndex++] == schemaNode.getDefinitionLevelMax()
            ? dataPage.getValues().get(valueIndex++)
            : null;

    advancePageIfNecessary();

    return result;
  }
}
