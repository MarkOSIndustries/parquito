package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkWriter;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;

public interface DataPageWriter<Value> extends ParquetPageWriter {
  static <Value> DataPageWriter<Value> create(
      final ColumnChunkWriter<Value> columnChunkWriter, PageType pageType) {
    return switch (pageType) {
        // TODO
        //        case DATA_PAGE -> new DataPageV1Writer<Value>(columnChunkWriter);
      case DATA_PAGE_V2 -> new DataPageV2Writer<Value>(columnChunkWriter);
      default -> throw new IllegalArgumentException("Unsupported data page type: " + pageType);
    };
  }

  void addNull(final int repetitionLevel, final int definitionLevel);

  void addValue(final Value value, final int repetitionLevel, final int definitionLevel);

  long getNumNulls(final PageHeader pageHeader);

  long getNumNulls();
}
