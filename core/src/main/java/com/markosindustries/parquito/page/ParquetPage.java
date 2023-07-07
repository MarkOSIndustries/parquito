package com.markosindustries.parquito.page;

import org.apache.parquet.format.PageHeader;

public interface ParquetPage<ReadAs> {
  PageHeader getPageHeader();

  int getTotalValues();

  int getNonNullValues();

  Values<ReadAs> getValues();
}
