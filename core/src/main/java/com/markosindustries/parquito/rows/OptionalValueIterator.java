package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetPredicate;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.RowReadSpec;
import com.markosindustries.parquito.page.DataPageReader;
import com.markosindustries.parquito.page.PredicateMatcher;
import java.util.Iterator;

public class OptionalValueIterator<ReadAs, Value> implements ParquetFieldIterator<ReadAs> {
  private final EOFDataPage<ReadAs> eofDataPage = new EOFDataPage<>();

  private final Iterator<DataPageReader<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final ParquetPredicate<ReadAs> predicate;
  private DataPageReader<ReadAs> dataPage = null;
  private PredicateMatcher dataPageMatcher = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public OptionalValueIterator(
      Iterator<DataPageReader<ReadAs>> dataPageIterator,
      ParquetSchemaNode schemaNode,
      RowReadSpec<?, Value, ReadAs> rowReadSpec) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    this.predicate = rowReadSpec.predicate();
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
  public boolean hasNext() {
    return dataPage != eofDataPage;
  }

  private void advancePageIfNecessary() {
    if (dataPage == null || definitionIndex == dataPage.getDefinitionLevels().length) {
      if (dataPageIterator.hasNext()) {
        dataPage = dataPageIterator.next();
        dataPageMatcher = dataPage.getValues().matcher(predicate);
      } else {
        dataPage = eofDataPage;
      }
      definitionIndex = 0;
      valueIndex = 0;
    }
  }

  @Override
  public boolean nextRowMatches() {
    if (dataPage.getDefinitionLevels()[definitionIndex] == schemaNode.getDefinitionLevelMax()) {
      return dataPageMatcher.matches(valueIndex);
    }
    // TODO - a way to match nulls?
    return false;
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
