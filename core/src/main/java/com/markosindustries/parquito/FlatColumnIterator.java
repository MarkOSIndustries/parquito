package com.markosindustries.parquito;

import com.markosindustries.parquito.page.DataPageReader;
import java.util.Iterator;

/**
 * Purely handles moving between pages seamlessly and leaves structure interpretation up to the
 * caller
 *
 * @param <ReadAs> The type of data to be read from the column data pages
 */
class FlatColumnIterator<ReadAs> implements Iterator<ReadAs> {
  private final Iterator<? extends DataPageReader<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private DataPageReader<ReadAs> dataPage = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public FlatColumnIterator(
      final Iterator<? extends DataPageReader<ReadAs>> dataPageIterator,
      final ParquetSchemaNode schemaNode) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    advancePageIfNecessary();
  }

  public int peekDefinitionLevel() {
    return dataPage.getDefinitionLevels()[definitionIndex];
  }

  public int peekRepetitionLevel() {
    return dataPage.getRepetitionLevels()[definitionIndex];
  }

  @Override
  public boolean hasNext() {
    return dataPage != null;
  }

  private void advancePageIfNecessary() {
    if (dataPage == null || definitionIndex == dataPage.getDefinitionLevels().length) {
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
            ? dataPage.getValues().get(valueIndex++)
            : null;

    advancePageIfNecessary();

    return result;
  }
}
