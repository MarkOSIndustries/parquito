package com.markosindustries.parquito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;

public class FileByteRangeReader implements ByteRangeReader {
  private final File file;
  private final Path path;

  public FileByteRangeReader(File file) {
    this.file = file;
    this.path = file.toPath();
  }

  @Override
  public long getTotalBytesAvailable() throws IOException {
    try (final var fileAccess = new RandomAccessFile(file, "r")) {
      return fileAccess.length();
    }
  }

  @Override
  public long readIntoBuffer(long startByteOffset, ByteBuffer buffer) throws IOException {
    try (final var channel = FileChannel.open(path, StandardOpenOption.READ)) {
      channel.position(startByteOffset);
      return channel.read(buffer);
    }
  }

  @Override
  public CompletableFuture<ByteBuffer> readAsBuffer(
      final long startByteOffset, final int bytesToRetrieve) {
    return CompletableFuture.supplyAsync(
        () -> {
          try (final var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, startByteOffset, bytesToRetrieve);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        Concurrency.DEFAULT_EXECUTOR);
  }

  @Override
  public void transferTo(
      long startByteOffset, int bytesToRetrieve, final WritableByteChannel destination)
      throws IOException {
    try (final var channel = FileChannel.open(path, StandardOpenOption.READ)) {
      if (bytesToRetrieve != channel.transferTo(startByteOffset, bytesToRetrieve, destination)) {
        throw new IOException("Not enough bytes were transferred");
      }
    }
  }

  @Override
  public void close() {}
}
