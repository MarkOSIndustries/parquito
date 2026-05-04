package com.markosindustries.parquito;

import com.markosindustries.parquito.schemas.Example;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;

public interface TestFileReader {
  void read(final ByteBuffer parquetFileBuffer, final Consumer<Iterator<Example>> iterateRows)
      throws Exception;
}
