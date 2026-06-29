package com.markosindustries.parquito;

import com.markosindustries.parquito.compression.SnappyCompressOnFlushOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import org.apache.parquet.format.CompressionCodec;
import org.xerial.snappy.Snappy;

public final class CompressionCodecs {
  @FunctionalInterface
  public interface StreamDecompressor {
    ByteBuffer decompress(ByteBuffer byteBuffer) throws IOException;
  }

  @FunctionalInterface
  public interface StreamCompressor {
    OutputStream compress(OutputStream outputStream) throws IOException;
  }

  public record Codec(StreamDecompressor streamDecompressor, StreamCompressor streamCompressor) {}

  private static final Map<CompressionCodec, Codec> REGISTERED_CODECS =
      new HashMap<>() {
        {
          put(
              CompressionCodec.UNCOMPRESSED,
              new Codec(byteBuffer -> byteBuffer, outputStream -> outputStream));
          put(
              CompressionCodec.SNAPPY,
              new Codec(
                  compressed -> {
                    if (compressed.isDirect()) {
                      final var uncompressed =
                          ByteBuffer.allocateDirect(Snappy.uncompressedLength(compressed));
                      Snappy.uncompress(compressed, uncompressed);
                      return uncompressed;
                    } else {
                      final var uncompressed =
                          ByteBuffer.allocate(
                              Snappy.uncompressedLength(
                                  compressed.array(),
                                  compressed.arrayOffset() + compressed.position(),
                                  compressed.remaining()));
                      Snappy.uncompress(
                          compressed.array(),
                          compressed.arrayOffset() + compressed.position(),
                          compressed.remaining(),
                          uncompressed.array(),
                          0);
                      return uncompressed;
                    }
                  },
                  SnappyCompressOnFlushOutputStream::new));
          put(
              CompressionCodec.GZIP,
              new Codec(
                  compressed -> {
                    // GZIPInputStream constructor reads the header (its size varies)
                    new GZIPInputStream(new ByteBufferInputStream(compressed)).close();

                    // GZIP puts the uncompressed size as a little endian int at the end of the
                    // trailer
                    final var uncompressedSize =
                        compressed
                            .slice()
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getInt(compressed.remaining() - 4);
                    final var uncompressed = ByteBuffer.allocateDirect(uncompressedSize);
                    final var gzip = new Inflater(true);
                    gzip.setInput(compressed);
                    try {
                      gzip.inflate(uncompressed);
                    } catch (DataFormatException e) {
                      throw new ParquetIOException(e);
                    }
                    return uncompressed.flip();
                  },
                  GZIPOutputStream::new));
        }
      };

  public static synchronized void register(
      final CompressionCodec compressionCodec, final Codec codec) {
    REGISTERED_CODECS.put(compressionCodec, codec);
  }

  private static Codec getCodec(final CompressionCodec compressionCodec) {
    final var codec = REGISTERED_CODECS.get(compressionCodec);
    if (codec == null) {
      throw new UnsupportedOperationException(
          "Use "
              + CompressionCodecs.class.getName()
              + ".register() to add support for "
              + CompressionCodec.class.getName()
              + " "
              + compressionCodec);
    }
    return codec;
  }

  public static ByteBuffer decompress(
      final CompressionCodec compressionCodec, final ByteBuffer byteBuffer) throws IOException {
    return getCodec(compressionCodec).streamDecompressor().decompress(byteBuffer);
  }

  public static OutputStream compress(
      final CompressionCodec compressionCodec, final OutputStream outputStream) throws IOException {
    return getCodec(compressionCodec).streamCompressor().compress(outputStream);
  }
}
