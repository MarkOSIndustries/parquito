package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.WriteSpec;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;

public interface DataPageWriter {
  static DataPageWriter create(
      final ColumnChunkWriter columnChunkWriter,
      final WriteSpec writeSpec,
      final PageType pageType) {
    return switch (pageType) {
        // TODO
        //        case DATA_PAGE -> new
        // DataPageV1Writer(columnChunkWriter.getColumnType().schemaNode(), writeSpec);
      case DATA_PAGE_V2 ->
          new DataPageV2Writer(columnChunkWriter.getColumnType().schemaNode(), writeSpec);
      default -> throw new IllegalArgumentException("Unsupported data page type: " + pageType);
    };
  }

  void addNull(final int repetitionLevel, final int definitionLevel);

  void addValue(final int repetitionLevel, final int definitionLevel);

  List<PageHeader> writePages(
      final ValueAccumulator.Slice values,
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
