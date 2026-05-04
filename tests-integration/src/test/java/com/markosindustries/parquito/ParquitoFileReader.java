package com.markosindustries.parquito;

import com.markosindustries.parquito.protobuf.ProtobufReader;
import com.markosindustries.parquito.schemas.Example;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.parquet.format.RowGroup;

public class ParquitoFileReader implements TestFileReader {
  @Override
  public void read(
      final ByteBuffer parquetFileBuffer, final Consumer<Iterator<Example>> iterateRows)
      throws Exception {
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      final var footer = ParquetFooter.read(byteRangeReader).join();
      final var schema = ParquetSchemaNode.from(footer.schema);
      for (RowGroup rowGroup : footer.row_groups) {
        final var rowGroupReader = new RowGroupReader(rowGroup, schema);
        final var rowIterator =
            rowGroupReader.getRowIterator(
                new RowReadSpec<>(new ProtobufReader<Example>(Example::newBuilder, schema)),
                byteRangeReader);
        iterateRows.accept(rowIterator);
      }
    }
  }

  @Override
  public String toString() {
    return ParquitoFileReader.class.getSimpleName();
  }
}
