package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ParquetIOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public interface DataPageReader extends ParquetPageReader {
  static DataPageReader create(
      final ColumnChunkReader columnChunkReader,
      final PageHeader pageHeader,
      final ByteBuffer pageBuffer) {
    try {
      return switch (pageHeader.type) {
        case DATA_PAGE -> new DataPageV1Reader(pageHeader, columnChunkReader, pageBuffer);
        case DATA_PAGE_V2 -> new DataPageV2Reader(pageHeader, columnChunkReader, pageBuffer);
        default ->
            throw new IllegalArgumentException("Unsupported data page type: " + pageHeader.type);
      };
    } catch (IOException e) {
      throw new ParquetIOException(e);
    }
  }

  int[] getRepetitionLevels();

  int[] getDefinitionLevels();
}
