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
    try (final var fileAccess = new RandomAccessFile(file, "r")) {
      fileAccess.seek(startByteOffset);
      if (buffer.hasArray()) {
        final var bytesRead =
            fileAccess.read(
                buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        if (bytesRead > -1) {
          buffer.position(buffer.position() + bytesRead);
        }
        return bytesRead;
      } else {
        final var someBuffer = new byte[Math.min(8192, buffer.remaining())];
        int totalBytesRead = 0;
        while (buffer.remaining() > 0) {
          final var bytesToRead = Math.min(someBuffer.length, buffer.remaining());
          final var bytesRead = fileAccess.read(someBuffer, 0, bytesToRead);
          if (bytesRead < 0) {
            return bytesRead;
          }
          totalBytesRead += bytesRead;
          buffer.put(someBuffer, 0, bytesRead);
          if (bytesRead < bytesToRead) {
            break;
          }
        }
        return totalBytesRead;
      }
    }
  }

  @Override
  public void close() {}
}
