package com.markosindustries.parquito;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.Nonnull;

public class ByteCountingOutputStream extends OutputStream {
  private final OutputStream outputStream;
  private long bytesWritten;

  public ByteCountingOutputStream(final OutputStream outputStream) {
    this.outputStream = outputStream;
    this.bytesWritten = 0;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  public int getBytesWrittenAsInt() {
    if (bytesWritten > Integer.MAX_VALUE) {
      throw new ParquetIOException(
          "Wrote more than " + Integer.MAX_VALUE + " bytes where parquet cannot support it");
    }
    return (int) bytesWritten;
  }

  @Override
  public void write(final int b) throws IOException {
    outputStream.write(b);
    bytesWritten++;
  }

  @Override
  public void write(final @Nonnull byte[] b) throws IOException {
    outputStream.write(b);
    bytesWritten += b.length;
  }

  @Override
  public void write(final @Nonnull byte[] b, final int off, final int len) throws IOException {
    outputStream.write(b, off, len);
    bytesWritten += len;
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
