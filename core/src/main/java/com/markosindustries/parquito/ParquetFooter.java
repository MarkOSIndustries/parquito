package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

public class ParquetFooter {
  private static final byte[] PARQUET_UNENCRYPTED_MAGIC_BYTES =
      "PAR1".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] PARQUET_ENCRYPTED_MAGIC_BYTES =
      "PARE".getBytes(StandardCharsets.US_ASCII);

  public static CompletableFuture<FileMetaData> read(ByteRangeReader byteRangeReader) {
    try {
      final long totalBytesAvailable = byteRangeReader.getTotalBytesAvailable();
      final var footerSizeAndMagicBufferOffset = totalBytesAvailable - 8;
      final var footerSizeAndMagicBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
      return byteRangeReader
          .readUntilFull(footerSizeAndMagicBufferOffset, footerSizeAndMagicBuffer)
          .thenCompose(
              unused -> {
                footerSizeAndMagicBuffer.flip();
                final var footerSize = footerSizeAndMagicBuffer.getInt();
                final var magic = new byte[4];
                footerSizeAndMagicBuffer.get(magic);
                final var footerIsEncrypted = isFooterEncrypted(magic);

                if (footerIsEncrypted) {
                  throw new RuntimeException("Encrypted reading is not currently implemented");
                }

                return byteRangeReader
                    .readAsInputStream(footerSizeAndMagicBufferOffset - footerSize, footerSize)
                    .thenApply(ParquetFooter::readFileMetaData);
              });
    } catch (IOException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  public static CompletableFuture<Void> write(FileMetaData metaData, OutputStream outputStream)
      throws IOException {
    return CompletableFuture.runAsync(
        () -> {
          try {
            final var countingOutputStream = new ByteCountingOutputStream(outputStream);
            Util.writeFileMetaData(metaData, countingOutputStream);

            final var footerSize = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            footerSize.putInt(countingOutputStream.getBytesWritten());

            outputStream.write(PARQUET_UNENCRYPTED_MAGIC_BYTES);
          } catch (IOException e) {
            throw new ParquetIOException(e);
          }
        });
  }

  static FileMetaData readFileMetaData(InputStream inputStream) {
    try {
      return Util.readFileMetaData(inputStream);
    } catch (IOException e) {
      throw new ParquetIOException(e);
    }
  }

  private static boolean isFooterEncrypted(byte[] magic) {
    if (Arrays.equals(PARQUET_UNENCRYPTED_MAGIC_BYTES, magic)) {
      return false;
    }
    if (Arrays.equals(PARQUET_ENCRYPTED_MAGIC_BYTES, magic)) {
      return true;
    }
    // TODO: include file path info or something here
    throw new RuntimeException(
        "Parquet magic bytes were not any of the expected values. This probably isn't a parquet file.");
  }
}
