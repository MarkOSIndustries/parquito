package com.markosindustries.parquito;

import com.markosindustries.parquito.compression.SnappyCompressOnFlushOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.anarres.lzo.LzoCompressor1x_1;
import org.anarres.lzo.LzoDecompressor1x;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoOutputStream;
import org.apache.parquet.format.CompressionCodec;
import org.brotli.dec.BrotliInputStream;
import org.xerial.snappy.SnappyInputStream;

public final class CompressionCodecs {
  @FunctionalInterface
  public interface StreamDecompressor {
    InputStream decompress(InputStream inputStream) throws IOException;
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
              new Codec(inputStream -> inputStream, outputStream -> outputStream));
          put(
              CompressionCodec.SNAPPY,
              new Codec(SnappyInputStream::new, SnappyCompressOnFlushOutputStream::new));
          put(
              CompressionCodec.GZIP,
              new Codec(
                  GZIPInputStream::new, outputStream -> new GZIPOutputStream(outputStream, true)));
          put(
              CompressionCodec.LZO,
              new Codec(
                  inputStream -> new LzoInputStream(inputStream, new LzoDecompressor1x()),
                  outputStream -> new LzoOutputStream(outputStream, new LzoCompressor1x_1())));
          put(
              CompressionCodec.BROTLI,
              new Codec(
                  BrotliInputStream::new,
                  outputStream -> {
                    throw new UnsupportedOperationException("No Brotli write support yet");
                  }));
          put(CompressionCodec.LZ4, new Codec(LZ4BlockInputStream::new, LZ4BlockOutputStream::new));
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

  public static InputStream decompress(
      final CompressionCodec compressionCodec, final InputStream inputStream) throws IOException {
    return getCodec(compressionCodec).streamDecompressor().decompress(inputStream);
  }

  public static OutputStream compress(
      final CompressionCodec compressionCodec, final OutputStream outputStream) throws IOException {
    return getCodec(compressionCodec).streamCompressor().compress(outputStream);
  }
}
