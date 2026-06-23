package com.markosindustries.parquito.page;

import org.apache.parquet.format.PageHeader;

public interface ParquetPageReader {
  PageHeader getPageHeader();

  int getTotalValues();

  int getNonNullValues();

  Values getValues();
}
