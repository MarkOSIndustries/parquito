package com.markosindustries.parquito;

import com.markosindustries.parquito.filesys.ByteBufferInputFile;
import com.markosindustries.parquito.schemas.Example;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.parquet.proto.ProtoParquetReader;

public class HadoopFileReader implements TestFileReader {
  @Override
  public void read(
      final ByteBuffer parquetFileBuffer, final Consumer<Iterator<Example>> iterateRows)
      throws Exception {
    try (final var reader =
        ProtoParquetReader.<Example.Builder>builder(new ByteBufferInputFile(parquetFileBuffer))
            .build()) {
      iterateRows.accept(
          new Iterator<Example>() {
            Example.Builder builder = reader.read();

            @Override
            public boolean hasNext() {
              return builder != null;
            }

            @Override
            public Example next() {
              final var next = builder.build();
              try {
                builder = reader.read();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return next;
            }
          });
    }
  }

  @Override
  public String toString() {
    return HadoopFileReader.class.getSimpleName();
  }
}
