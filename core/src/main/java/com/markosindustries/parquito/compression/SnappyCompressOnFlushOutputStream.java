package com.markosindustries.parquito.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

/**
 * This class is necessary because {@link SnappyOutputStream} generates output which is incompatible
 * with {@link Snappy#uncompress}.
 *
 * <p>See <a href="https://github.com/xerial/snappy-java#data-format-compatibility-matrix">Snappy
 * compatibility matrix</a>
 *
 * <p>The hadoop parquet library decompresses snappy pages using {@link Snappy#uncompress}, so we
 * must use {@link Snappy#compress} when writing in order to be compliant
 */
public class SnappyCompressOnFlushOutputStream extends ByteArrayOutputStream {
  private final OutputStream outputStream;

  public SnappyCompressOnFlushOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void flush() throws IOException {
    byte[] compressed = new byte[2 + count];
    final var compressedBytes = Snappy.compress(buf, 0, count, compressed, 0);
    outputStream.write(compressed, 0, compressedBytes);
    count = 0;
  }

  @Override
  public void close() throws IOException {
    flush();
  }
}
