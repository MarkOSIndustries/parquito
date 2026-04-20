package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkWriter;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;

public interface DataPageWriter<Value> {
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

  PageHeader writePage(final ColumnMetaData columnMetaData, final OutputStream outputStream)
      throws IOException;

  long getNumValues(final PageHeader pageHeader);

  long getNumNulls(final PageHeader pageHeader);
}
