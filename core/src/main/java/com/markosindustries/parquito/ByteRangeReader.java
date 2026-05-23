package com.markosindustries.parquito;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

public interface ByteRangeReader extends AutoCloseable {
  long getTotalBytesAvailable() throws IOException;

  long readIntoBuffer(long startByteOffset, ByteBuffer buffer) throws IOException;

  default CompletableFuture<ByteBuffer> readUntilFull(long startByteOffset, ByteBuffer buffer) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            long nextOffset = startByteOffset;
            long lastBytesRead = 0;
            while (buffer.remaining() > 0) {
              lastBytesRead = readIntoBuffer(nextOffset, buffer);
              if (lastBytesRead == -1) {
                break;
              }
              nextOffset += lastBytesRead;
            }

            if (buffer.remaining() > 0) {
              throw new EOFException("Not enough bytes could be read to fill the buffer");
            }

            return buffer;
          } catch (IOException ioException) {
            throw new ParquetIOException(ioException);
          }
        },
        Concurrency.DEFAULT_EXECUTOR);
  }

  default CompletableFuture<ByteBuffer> readAsBuffer(long startByteOffset, int bytesToRetrieve) {
    return readUntilFull(startByteOffset, ByteBuffer.allocate(bytesToRetrieve))
        .thenApplyAsync(
            buffer -> {
              buffer.flip();
              return buffer;
            },
            Concurrency.DEFAULT_EXECUTOR);
  }

  default CompletableFuture<InputStream> readAsInputStream(
      long startByteOffset, int bytesToRetrieve) {
    return readAsBuffer(startByteOffset, bytesToRetrieve)
        .thenApplyAsync(
            buffer -> {
              if (buffer.hasArray()) {
                return new ByteArrayInputStream(buffer.array(), 0, bytesToRetrieve);
              } else {
                return new ByteBufferInputStream(buffer);
              }
            },
            Concurrency.DEFAULT_EXECUTOR);
  }

  void transferTo(long startByteOffset, int bytesToRetrieve, WritableByteChannel destination)
      throws IOException;
}
