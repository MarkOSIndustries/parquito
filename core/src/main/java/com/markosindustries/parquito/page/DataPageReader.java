package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ParquetIOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.format.PageHeader;

public interface DataPageReader<ReadAs> extends ParquetPageReader<ReadAs> {
  static <ReadAs> DataPageReader<ReadAs> create(
      final ColumnChunkReader<ReadAs> columnChunkReader,
      final PageHeader pageHeader,
      final ByteBuffer pageBuffer) {
    try {
      return switch (pageHeader.type) {
        case DATA_PAGE -> new DataPageV1Reader<ReadAs>(pageHeader, columnChunkReader, pageBuffer);
        case DATA_PAGE_V2 ->
            new DataPageV2Reader<ReadAs>(pageHeader, columnChunkReader, pageBuffer);
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
