package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ByteBufferByteRangeReader implements ByteRangeReader {
  private final ByteBuffer byteBuffer;

  public ByteBufferByteRangeReader(final ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public long getTotalBytesAvailable() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public long readIntoBuffer(final long startByteOffset, final ByteBuffer buffer)
      throws IOException {
    final var bytesToRead =
        Math.min(buffer.remaining(), byteBuffer.remaining() - (int) startByteOffset);
    buffer.put(byteBuffer.slice((int) startByteOffset, bytesToRead));
    return bytesToRead;
  }

  @Override
  public CompletableFuture<ByteBuffer> readUntilFull(
      final long startByteOffset, final ByteBuffer buffer) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            readIntoBuffer(startByteOffset, buffer);
            return buffer;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        Concurrency.DEFAULT_EXECUTOR);
  }

  @Override
  public CompletableFuture<ByteBuffer> readAsBuffer(
      final long startByteOffset, final int bytesToRetrieve) {
    return CompletableFuture.supplyAsync(
        () -> byteBuffer.slice((int) startByteOffset, bytesToRetrieve));
  }

  @Override
  public CompletableFuture<InputStream> readAsInputStream(
      final long startByteOffset, final int bytesToRetrieve) {
    return CompletableFuture.supplyAsync(
        () -> new ByteBufferInputStream(byteBuffer.slice((int) startByteOffset, bytesToRetrieve)));
  }

  @Override
  public void close() throws Exception {}
}
