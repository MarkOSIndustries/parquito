package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.page.DataPageReader;
import com.markosindustries.parquito.page.Values;
import org.apache.parquet.format.PageHeader;

public class EOFDataPage implements DataPageReader {
  private static final int[] LEVELS = new int[] {0};

  @Override
  public int[] getRepetitionLevels() {
    return LEVELS;
  }

  @Override
  public int[] getDefinitionLevels() {
    return LEVELS;
  }

  @Override
  public PageHeader getPageHeader() {
    return null;
  }

  @Override
  public int getTotalValues() {
    return 0;
  }

  @Override
  public int getNonNullValues() {
    return 0;
  }

  @Override
  public Values getValues() {
    return Values.empty();
  }
}
