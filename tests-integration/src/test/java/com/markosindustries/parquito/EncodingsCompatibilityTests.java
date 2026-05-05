package com.markosindustries.parquito;

import static org.apache.parquet.proto.ProtoReadSupport.PB_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.markosindustries.parquito.protobuf.ProtobufParquetConfig;
import com.markosindustries.parquito.protobuf.ProtobufWriter;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EncodingsCompatibilityTests {
  private static final List<CompressionCodec> COMPRESSION_CODECS =
      List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);

  private static Stream<Arguments> byteEncodingsSource() {
    final var encodings =
        List.of(
            Encoding.PLAIN,
            Encoding.DELTA_BYTE_ARRAY,
            Encoding.DELTA_LENGTH_BYTE_ARRAY,
            Encoding.RLE_DICTIONARY);
    final var valueCounts = List.of(1, 10_000);
    final var byteMutationsPerRow = List.of(0, 1, 3);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()),
                COMPRESSION_CODECS,
                encodings,
                valueCounts,
                byteMutationsPerRow)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()),
                COMPRESSION_CODECS,
                encodings,
                valueCounts,
                byteMutationsPerRow)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> intEncodingsSource() {
    final var encodings =
        List.of(
            Encoding.PLAIN,
            Encoding.DELTA_BINARY_PACKED,
            Encoding.RLE_DICTIONARY,
            Encoding.BYTE_STREAM_SPLIT);
    final var valueCounts = List.of(1, 10_000);
    final var maxValues = List.of(0, 1, Integer.MAX_VALUE);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()),
                COMPRESSION_CODECS,
                List.of(
                    Encoding.PLAIN,
                    Encoding.DELTA_BINARY_PACKED,
                    Encoding.RLE_DICTIONARY,
                    Encoding.BYTE_STREAM_SPLIT),
                valueCounts,
                maxValues)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()),
                COMPRESSION_CODECS,
                List.of(Encoding.PLAIN, Encoding.DELTA_BINARY_PACKED, Encoding.RLE_DICTIONARY),
                valueCounts,
                maxValues)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> longEncodingsSource() {
    final var valueCounts = List.of(1, 10_000);
    final var maxValues = List.of(0, 1, Long.MAX_VALUE);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()),
                COMPRESSION_CODECS,
                List.of(
                    Encoding.PLAIN,
                    Encoding.DELTA_BINARY_PACKED,
                    Encoding.RLE_DICTIONARY,
                    Encoding.BYTE_STREAM_SPLIT),
                valueCounts,
                maxValues)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()),
                COMPRESSION_CODECS,
                List.of(Encoding.PLAIN, Encoding.DELTA_BINARY_PACKED, Encoding.RLE_DICTIONARY),
                valueCounts,
                maxValues)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> floatEncodingsSource() {
    final var encodings =
        List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.BYTE_STREAM_SPLIT);
    final var valueCounts = List.of(1, 10_000);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()), COMPRESSION_CODECS, encodings, valueCounts)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()), COMPRESSION_CODECS, encodings, valueCounts)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> doubleEncodingsSource() {
    final var encodings =
        List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.BYTE_STREAM_SPLIT);
    final var valueCounts = List.of(1, 10_000);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()), COMPRESSION_CODECS, encodings, valueCounts)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()), COMPRESSION_CODECS, encodings, valueCounts)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> boolEncodingsSource() {
    final var valueCounts = List.of(1, 10_000);

    return Stream.concat(
            Lists.cartesianProduct(
                List.of(new ParquitoFileReader()),
                COMPRESSION_CODECS,
                List.of(Encoding.PLAIN, Encoding.RLE, Encoding.RLE_DICTIONARY),
                valueCounts)
                .stream(),
            Lists.cartesianProduct(
                List.of(new HadoopFileReader()),
                COMPRESSION_CODECS,
                List.of(Encoding.RLE),
                valueCounts)
                .stream())
        .map(args -> Arguments.of(args.toArray()));
  }

  static final String RANDOM_STRING_CHARACTERS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

  private char randomChar(final Random random) {
    return RANDOM_STRING_CHARACTERS.charAt(random.nextInt(RANDOM_STRING_CHARACTERS.length()));
  }

  private char[] randomString(final Random random, final int length) {
    char[] str = new char[length];
    for (var i = 0; i < length; i++) {
      str[i] = randomChar(random);
    }
    return str;
  }

  @ParameterizedTest
  @MethodSource("byteEncodingsSource")
  public void byteEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount,
      final int byteMutationsPerRow)
      throws Exception {
    final var random = new Random();

    final var str = randomString(random, 34);

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 1
              && columnMetaData.path_in_schema.get(0).equals("some_string")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          for (var i = 0; i < byteMutationsPerRow; i++) {
            final var mutateIndex = random.nextInt(str.length);
            str[mutateIndex] = randomChar(random);
          }
          return Example.newBuilder().setSomeString(new String(str)).build();
        });
  }

  @ParameterizedTest
  @MethodSource("intEncodingsSource")
  public void intEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount,
      final int maxValue)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 2
              && columnMetaData.path_in_schema.get(1).equals("some_int32")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          return Example.newBuilder()
              .setSomeChild(
                  ExampleChild.newBuilder()
                      .setSomeInt32(maxValue == 0 ? 0 : random.nextInt(maxValue))
                      .build())
              .build();
        });
  }

  @ParameterizedTest
  @MethodSource("longEncodingsSource")
  public void longEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount,
      final long maxValue)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 2
              && columnMetaData.path_in_schema.get(1).equals("some_int64")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          return Example.newBuilder()
              .setSomeChild(
                  ExampleChild.newBuilder()
                      .setSomeInt64(maxValue == 0 ? 0 : random.nextLong(maxValue))
                      .build())
              .build();
        });
  }

  @ParameterizedTest
  @MethodSource("floatEncodingsSource")
  public void floatEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 2
              && columnMetaData.path_in_schema.get(1).equals("some_float")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          return Example.newBuilder()
              .setSomeChild(
                  ExampleChild.newBuilder().setSomeFloat(random.nextFloat(Float.MAX_VALUE)).build())
              .build();
        });
  }

  @ParameterizedTest
  @MethodSource("doubleEncodingsSource")
  public void doubleEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 2
              && columnMetaData.path_in_schema.get(1).equals("some_double")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          return Example.newBuilder()
              .setSomeChild(
                  ExampleChild.newBuilder()
                      .setSomeDouble(random.nextDouble(Double.MAX_VALUE))
                      .build())
              .build();
        });
  }

  @ParameterizedTest
  @MethodSource("boolEncodingsSource")
  public void boolEncodingsRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
        testFileReader,
        codec,
        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
          if (columnMetaData.path_in_schema.size() == 1
              && columnMetaData.path_in_schema.get(0).equals("some_bool")) {
            return encoding;
          }
          return Encoding.PLAIN;
        },
        valueCount,
        () -> {
          return Example.newBuilder().setSomeBool(random.nextBoolean()).build();
        });
  }

  private void encodingRoundTrip(
      final TestFileReader testFileReader,
      final CompressionCodec compressionCodec,
      final EncodingSelector encodingSelector,
      final int valueCount,
      final Supplier<Example> randomRowSupplier)
      throws Exception {
    final var expectedProtobufs =
        IntStream.range(0, valueCount).mapToObj(ignored -> randomRowSupplier.get()).toList();

    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder()
                .withCompressionCodec(compressionCodec)
                .withEncodingSelector(encodingSelector)
                .build(),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build()))) {
      writer.putMetaData(PB_CLASS, Example.class.getName());
      writer.write(expectedProtobufs.iterator());
    }

    final var rowIndex = new AtomicInteger(0);
    testFileReader.read(
        outputStream.asByteBuffer(),
        (Iterator<Example> rowIterator) -> {
          while (rowIterator.hasNext()) {
            final var row = rowIterator.next();
            final var expectedProtobuf = expectedProtobufs.get(rowIndex.get());
            assertEquals(expectedProtobuf, row, "Row " + rowIndex + " did not match");
            rowIndex.incrementAndGet();
          }
        });
    assertEquals(expectedProtobufs.size(), rowIndex.get(), "Row count did not match");
  }
}
