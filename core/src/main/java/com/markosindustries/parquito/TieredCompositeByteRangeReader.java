package com.markosindustries.parquito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

/**
 * Reads from underlying {@link ByteRangeReader}s for a given byte range, with a fallback for when
 * none is specified for a given byte position.
 */
public class TieredCompositeByteRangeReader implements ByteRangeReader {
  private final ByteRangeReader preferred;
  private final long preferredStartInclusive;
  private final long preferredEndExclusive;
  private final ByteRangeReader fallback;

  public TieredCompositeByteRangeReader(
      final ByteRangeReader preferred,
      final long preferredStartInclusive,
      final long preferredEndExclusive,
      final ByteRangeReader fallback) {
    this.preferred = preferred;
    this.preferredStartInclusive = preferredStartInclusive;
    this.preferredEndExclusive = preferredEndExclusive;
    this.fallback = fallback;
  }

  @Override
  public long getTotalBytesAvailable() throws IOException {
    return fallback.getTotalBytesAvailable();
  }

  @Override
  public long readIntoBuffer(long startByteOffset, ByteBuffer buffer) throws IOException {
    if (startByteOffset >= preferredStartInclusive
        && startByteOffset + buffer.remaining() <= preferredEndExclusive) {
      return preferred.readIntoBuffer(startByteOffset, buffer);
    }
    return fallback.readIntoBuffer(startByteOffset, buffer);
  }

  @Override
  public CompletableFuture<ByteBuffer> readAsBuffer(
      final long startByteOffset, final int bytesToRetrieve) {
    if (startByteOffset >= preferredStartInclusive
        && startByteOffset + bytesToRetrieve <= preferredEndExclusive) {
      return preferred.readAsBuffer(startByteOffset, bytesToRetrieve);
    }
    return fallback.readAsBuffer(startByteOffset, bytesToRetrieve);
  }

  @Override
  public void transferTo(
      final long startByteOffset, final int bytesToRetrieve, final WritableByteChannel destination)
      throws IOException {
    if (startByteOffset >= preferredStartInclusive
        && startByteOffset + bytesToRetrieve <= preferredEndExclusive) {
      preferred.transferTo(startByteOffset, bytesToRetrieve, destination);
    }
    fallback.transferTo(startByteOffset, bytesToRetrieve, destination);
  }

  @Override
  public void close() {}
}
