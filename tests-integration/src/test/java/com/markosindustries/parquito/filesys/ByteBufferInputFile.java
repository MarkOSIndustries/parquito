package com.markosindustries.parquito.filesys;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ByteBufferInputFile implements InputFile {
  private final ByteBuffer parquetFileBuffer;

  public ByteBufferInputFile(final ByteBuffer parquetFileBuffer) {
    this.parquetFileBuffer = parquetFileBuffer;
  }

  @Override
  public long getLength() throws IOException {
    return parquetFileBuffer.remaining();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new ByteBufferBasedSeekableInputStream(parquetFileBuffer);
  }
}
