package com.markosindustries.parquito.page;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;

public interface ParquetPageWriter {
  PageHeader writePage(
      final Encoding encoding, final ColumnMetaData columnMetaData, final OutputStream outputStream)
      throws IOException;

  long getNumValues(final PageHeader pageHeader);

  long getNumValues();
}
