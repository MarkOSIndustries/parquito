package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunk;
import com.markosindustries.parquito.ParquetIOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public interface DataPage<ReadAs extends Comparable<ReadAs>> extends ParquetPage<ReadAs> {
  static <ReadAs extends Comparable<ReadAs>> DataPage<ReadAs> create(
      final ColumnChunk<ReadAs> columnChunk,
      final PageHeader pageHeader,
      final ByteBuffer pageBuffer) {
    try {
      return switch (pageHeader.type) {
        case DATA_PAGE -> new DataPageV1<ReadAs>(pageHeader, columnChunk, pageBuffer);
        case DATA_PAGE_V2 -> new DataPageV2<ReadAs>(pageHeader, columnChunk, pageBuffer);
        default -> throw new IllegalArgumentException(
            "Unsupported data page type: " + pageHeader.type);
      };
    } catch (IOException e) {
      throw new ParquetIOException(e);
    }
  }

  int[] getRepetitionLevels();

  int[] getDefinitionLevels();
}
