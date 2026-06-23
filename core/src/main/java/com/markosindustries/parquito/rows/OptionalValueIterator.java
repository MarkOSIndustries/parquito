package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.DataPageReader;
import java.util.Iterator;

public class OptionalValueIterator implements ParquetFieldIterator, DataPageCursor {
  private final EOFDataPage eofDataPage = new EOFDataPage();

  private final Iterator<DataPageReader> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final PushdownPredicates pushdownPredicates;

  private DataPageReader dataPage = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public OptionalValueIterator(
      Iterator<DataPageReader> dataPageIterator,
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
  public DataPageReader getDataPage() {
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
  public void visitNext(final FieldVisitor visitor) {
    final var pageIndex = definitionIndex++;
    if (dataPage.getDefinitionLevels()[pageIndex] == schemaNode.getDefinitionLevelMax()) {
      dataPage.getValues().visit(pageIndex, valueIndex++, visitor);
    } else {
      visitor.visitNull(pageIndex);
    }

    advancePageIfNecessary();
  }
}
