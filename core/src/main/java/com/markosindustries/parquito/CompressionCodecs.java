package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.anarres.lzo.LzoDecompressor1x;
import org.anarres.lzo.LzoInputStream;
import org.apache.parquet.format.CompressionCodec;
import org.brotli.dec.BrotliInputStream;
import org.xerial.snappy.SnappyInputStream;

public final class CompressionCodecs {
  @FunctionalInterface
  public interface StreamDecompressor {
    InputStream decompress(InputStream inputStream) throws IOException;
  }

  private static final Map<CompressionCodec, StreamDecompressor> REGISTERED_CODECS =
      new HashMap<>() {
        {
          put(CompressionCodec.UNCOMPRESSED, inputStream -> inputStream);
          put(CompressionCodec.SNAPPY, SnappyInputStream::new);
          put(CompressionCodec.GZIP, GZIPInputStream::new);
          put(
              CompressionCodec.LZO,
              inputStream -> new LzoInputStream(inputStream, new LzoDecompressor1x()));
          put(CompressionCodec.BROTLI, BrotliInputStream::new);
          put(CompressionCodec.LZ4, LZ4BlockInputStream::new);
        }
      };

  public static void register(
      CompressionCodec compressionCodec, StreamDecompressor streamDecompressor) {
    REGISTERED_CODECS.put(compressionCodec, streamDecompressor);
  }

  public static InputStream decompress(CompressionCodec compressionCodec, InputStream inputStream)
      throws IOException {
    final var decompressor = REGISTERED_CODECS.get(compressionCodec);
    if (decompressor == null) {
      throw new UnsupportedEncodingException(
          "Use CompressionCodecs.register to add support for codec " + compressionCodec);
    }
    return decompressor.decompress(inputStream);
  }
}
