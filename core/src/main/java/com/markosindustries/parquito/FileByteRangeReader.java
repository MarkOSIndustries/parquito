package com.markosindustries.parquito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;

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
  public CompletableFuture<ByteBuffer> readAsBuffer(
      final long startByteOffset, final int bytesToRetrieve) {
    return CompletableFuture.supplyAsync(
        () -> {
          try (final var fileAccess = new RandomAccessFile(file, "r");
              final var channel = fileAccess.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, startByteOffset, bytesToRetrieve);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        Concurrency.DEFAULT_EXECUTOR);
  }

  @Override
  public void close() {}
}
