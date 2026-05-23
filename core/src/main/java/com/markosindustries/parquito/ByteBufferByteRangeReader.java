package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

public class ByteBufferByteRangeReader implements ByteRangeReader {
  private final ByteBuffer byteBuffer;
  private final long offset;

  public ByteBufferByteRangeReader(final ByteBuffer byteBuffer) {
    this(byteBuffer, 0L);
  }

  public ByteBufferByteRangeReader(final ByteBuffer byteBuffer, final long offset) {
    this.byteBuffer = byteBuffer;
    this.offset = offset;
  }

  @Override
  public long getTotalBytesAvailable() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public long readIntoBuffer(final long startByteOffset, final ByteBuffer buffer)
      throws IOException {
    final long actualStartOffset = startByteOffset - offset;
    final var bytesToRead =
        Math.min(buffer.remaining(), byteBuffer.remaining() - (int) actualStartOffset);
    buffer.put(byteBuffer.slice((int) actualStartOffset, bytesToRead));
    return bytesToRead;
  }

  @Override
  public CompletableFuture<ByteBuffer> readUntilFull(
      final long startByteOffset, final ByteBuffer buffer) {
    final long actualStartOffset = startByteOffset - offset;
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            readIntoBuffer(actualStartOffset, buffer);
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
    final long actualStartOffset = startByteOffset - offset;
    return CompletableFuture.supplyAsync(
        () -> byteBuffer.slice((int) actualStartOffset, bytesToRetrieve));
  }

  @Override
  public CompletableFuture<InputStream> readAsInputStream(
      final long startByteOffset, final int bytesToRetrieve) {
    final long actualStartOffset = startByteOffset - offset;
    return CompletableFuture.supplyAsync(
        () ->
            new ByteBufferInputStream(byteBuffer.slice((int) actualStartOffset, bytesToRetrieve)));
  }

  @Override
  public void transferTo(
      final long startByteOffset, final int bytesToRetrieve, final WritableByteChannel destination)
      throws IOException {
    final long actualStartOffset = startByteOffset - offset;
    destination.write(byteBuffer.slice((int) actualStartOffset, bytesToRetrieve));
  }

  @Override
  public void close() throws Exception {}
}
