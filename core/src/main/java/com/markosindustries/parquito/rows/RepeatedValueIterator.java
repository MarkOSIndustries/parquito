package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetPredicate;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.RowReadSpec;
import com.markosindustries.parquito.page.DataPage;
import com.markosindustries.parquito.page.PredicateMatcher;
import java.util.Iterator;

public class RepeatedValueIterator<ReadAs, Repeated, Value>
    implements ParquetFieldIterator<Repeated> {
  private final Iterator<DataPage<ReadAs>> dataPageIterator;
  private final ParquetSchemaNode schemaNode;
  private final Reader<Repeated, Value> reader;
  private final ParquetPredicate<ReadAs> predicate;
  private DataPage<ReadAs> dataPage = null;
  private PredicateMatcher dataPageMatcher = null;
  private int valueIndex = 0;
  private int definitionIndex = 0;

  public RepeatedValueIterator(
      Iterator<DataPage<ReadAs>> dataPageIterator,
      ParquetSchemaNode schemaNode,
      RowReadSpec<Repeated, Value, ReadAs> rowReadSpec) {
    this.dataPageIterator = dataPageIterator;
    this.schemaNode = schemaNode;
    this.reader = rowReadSpec.reader();
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
    return dataPage != null;
  }

  private void advancePageIfNecessary() {
    if (dataPage == null || definitionIndex == dataPage.getDefinitionLevels().length) {
      if (dataPageIterator.hasNext()) {
        dataPage = dataPageIterator.next();
        dataPageMatcher = dataPage.getValues().matcher(predicate);
      } else {
        dataPage = null;
      }
      definitionIndex = 0;
      valueIndex = 0;
    }
  }

  @Override
  public boolean nextRowMatches() {
    int dIndex = definitionIndex, vIndex = valueIndex;
    do {
      if (dataPage.getDefinitionLevels()[dIndex] == schemaNode.getDefinitionLevelMax()) {
        if (dataPageMatcher.matches(vIndex++)) {
          return true;
        }
      }
      dIndex++;
    } while (dIndex < dataPage.getDefinitionLevels().length
        && dataPage.getRepetitionLevels()[dIndex] != 0);

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
  public Repeated next() {
    final var values = reader.repeatedBuilder();
    do {
      if (dataPage.getDefinitionLevels()[definitionIndex] == schemaNode.getDefinitionLevelMax()) {
        //noinspection unchecked
        values.add((Value) dataPage.getValues().get(valueIndex++));
      }
      definitionIndex++;
    } while (definitionIndex < dataPage.getDefinitionLevels().length
        && dataPage.getRepetitionLevels()[definitionIndex] == schemaNode.getRepetitionLevelMax());
    advancePageIfNecessary();

    return values.build();
  }
}
