package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetSchemaPath;

public interface ValueAccumulator {
  ColumnChunkWriter<?> getColumnChunkWriter(final ParquetSchemaPath parquetSchemaPath);
}
