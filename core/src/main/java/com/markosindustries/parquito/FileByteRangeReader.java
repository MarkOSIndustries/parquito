package com.markosindustries.parquito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class FileByteRangeReader implements ByteRangeReader {
  private final File file;

  public FileByteRangeReader(File file) {
    this.file = file;
  }

  @Override
  public long getTotalBytesAvailable() throws IOException {
    try (final var fileAccess = new RandomAccessFile(file, "r")) {
      return fileAccess.length();
    }
  }

  @Override
  public long readIntoBuffer(long startByteOffset, ByteBuffer buffer) throws IOException {
    try (final var fileAccess = new RandomAccessFile(file, "r");
        final var channel = fileAccess.getChannel()) {
      fileAccess.seek(startByteOffset); // also moves the channel
      return channel.read(buffer);
    }
  }

  @Override
  public void close() {}
}
