package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.WriteSpec;
import com.markosindustries.parquito.arrays.FastDictionary;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;

public interface DataPageWriter<Value> {
  static <Value> DataPageWriter<Value> create(
      final ColumnChunkWriter<Value> columnChunkWriter,
      final WriteSpec writeSpec,
      final PageType pageType) {
    return switch (pageType) {
        // TODO
        //        case DATA_PAGE -> new DataPageV1Writer<Value>(columnChunkWriter);
      case DATA_PAGE_V2 -> new DataPageV2Writer<Value>(columnChunkWriter, writeSpec);
      default -> throw new IllegalArgumentException("Unsupported data page type: " + pageType);
    };
  }

  void addNull(final int repetitionLevel, final int definitionLevel);

  void addValue(final int repetitionLevel, final int definitionLevel);

  List<PageHeader> writePages(
      final FastDictionary<Value, ?> values,
      final int estimatedPlainBytesRequired,
      final Encoding encoding,
      final ColumnMetaData columnMetaData,
      final OutputStream outputStream)
      throws IOException;

  long getNumValues(final PageHeader pageHeader);

  long getNumValues();

  long getNumNulls(final PageHeader pageHeader);

  long getNumNulls();

  long getNumRows(final PageHeader pageHeader);

  long getNumRows();
}
