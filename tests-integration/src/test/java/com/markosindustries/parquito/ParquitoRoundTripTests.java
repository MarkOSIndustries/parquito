package com.markosindustries.parquito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.protobuf.ProtobufParquetConfig;
import com.markosindustries.parquito.protobuf.ProtobufReader;
import com.markosindustries.parquito.protobuf.ProtobufWriter;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests where parquito does both the writing and reading */
public class ParquitoRoundTripTests {
  private static Stream<Arguments> writerConfigCombinations() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);

    return Lists.cartesianProduct(compressionCodecs).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canWriteAndReadEmptyFile(CompressionCodec compressionCodec) throws Exception {
    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder().withCompressionCodec(compressionCodec).build(),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), new ProtobufParquetConfig(false)))) {
      writer.write(Collections.emptyIterator());
    }

    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                var rowIndex = 0;
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new ProtobufReader<>(Example::newBuilder, schema)),
                          byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var row = rowIterator.next();
                    rowIndex++;
                  }
                }
                assertEquals(0, rowIndex, "Row count did not match");
              })
          .join();
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void writeProtobufsThenReadThem(CompressionCodec compressionCodec) throws Exception {
    final var expectedProtobufs =
        List.of(
            Example.newBuilder()
                .setSomeChild(
                    ExampleChild.newBuilder()
                        .setSomeString("stra")
                        .addAllSomeStrings(List.of("str1", "str2"))
                        .setSomeInt32(Integer.MAX_VALUE - 465231)
                        .setSomeInt64(Integer.MAX_VALUE + 465231L)
                        .setSomeFloat(Float.MAX_VALUE - 328746.23462F)
                        .setSomeDouble(Float.MAX_VALUE + 328746.23462D)
                        .setSomeBinary(ByteString.copyFromUtf8("just some bytes")))
                .addAllSomeRepeated(
                    List.of(
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr1")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_TWO)
                            .build(),
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr2")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_ONE)
                            .build()))
                .build(),
            Example.newBuilder()
                .setSomeChild(
                    ExampleChild.newBuilder()
                        .setSomeString("strb")
                        .addAllSomeStrings(List.of("str1", "str2"))
                        .setSomeInt32(Integer.MAX_VALUE - 872634)
                        .setSomeInt64(Integer.MAX_VALUE + 872634L)
                        .setSomeFloat(Float.MAX_VALUE - 9837465.23462F)
                        .setSomeDouble(Float.MAX_VALUE + 9837465.23462D)
                        .setSomeBinary(ByteString.copyFromUtf8("just some bytes")))
                .addAllSomeRepeated(
                    List.of(
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr1")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_TWO)
                            .build(),
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr2")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_ONE)
                            .build()))
                .build(),
            Example.newBuilder()
                .setSomeChild(
                    ExampleChild.newBuilder()
                        .setSomeString("strc")
                        .addAllSomeStrings(List.of("str1", "str2"))
                        .setSomeInt32(Integer.MAX_VALUE - 974456)
                        .setSomeInt64(Integer.MAX_VALUE + 974456L)
                        .setSomeFloat(Float.MAX_VALUE - 102987.23462F)
                        .setSomeDouble(Float.MAX_VALUE + 102987.23462D)
                        .setSomeBinary(ByteString.copyFromUtf8("just some bytes")))
                .addAllSomeRepeated(
                    List.of(
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr1")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_TWO)
                            .build(),
                        Example.ExampleRepeated.newBuilder()
                            .setSomeString("strrr2")
                            .setSomeEnum(ExampleEnum.EXAMPLE_ENUM_ONE)
                            .build()))
                .build(),
            Example.newBuilder().build());

    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder()
                .withTargetBytesPerRowGroup(10)
                .withCompressionCodec(compressionCodec)
                .withBloomFilterSelector(
                    (columnMetaData, distinctValues, totalValues, totalNulls) ->
                        columnMetaData.path_in_schema.contains("some_string"))
                .build(),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), new ProtobufParquetConfig(false)))) {
      writer.write(expectedProtobufs.iterator());
    }

    outputStream.writeTo(
        Files.newOutputStream(Path.of("/tmp/roundtrip.protobuf." + compressionCodec + ".parquet")));
    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                var rowIndex = 0;
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final ColumnChunkReader<?> someStringColumnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(
                              byteRangeReader,
                              ParquetSchemaPath.parseDotSeparatedPath(
                                  schema, "some_child.some_string"))
                          .get();
                  assertTrue(someStringColumnChunkReader.hasBloomFilter());
                  assertFalse(someStringColumnChunkReader.getBloomFilter().mightContain("strx"));
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new ProtobufReader<>(Example::newBuilder, schema)),
                          byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var row = rowIterator.next();
                    final var expectedProtobuf = expectedProtobufs.get(rowIndex);
                    if (expectedProtobuf.hasSomeChild()) {
                      assertTrue(
                          someStringColumnChunkReader
                              .getBloomFilter()
                              .mightContain(expectedProtobuf.getSomeChild().getSomeString()),
                          "Bloom filter did not contain "
                              + expectedProtobuf.getSomeChild().getSomeString());
                    }
                    assertEquals(expectedProtobuf, row, "Row " + rowIndex + " did not match");
                    rowIndex++;
                  }
                }
                assertEquals(expectedProtobufs.size(), rowIndex, "Row count did not match");
              })
          .join();
    }
  }

  private static Stream<Arguments> byteEncodingsSource() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);
    final var encodings =
        List.of(
            Encoding.PLAIN,
            Encoding.DELTA_BYTE_ARRAY,
            Encoding.DELTA_LENGTH_BYTE_ARRAY,
            Encoding.RLE_DICTIONARY);
    final var valueCounts = List.of(1, 10_000);

    return Lists.cartesianProduct(compressionCodecs, encodings, valueCounts).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> intEncodingsSource() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);
    final var encodings =
        List.of(Encoding.PLAIN, Encoding.DELTA_BINARY_PACKED, Encoding.RLE_DICTIONARY /*,
            Encoding.BYTE_STREAM_SPLIT*/);
    final var valueCounts = List.of(1, 10_000);
    final var maxValues = List.of(0, 1, Integer.MAX_VALUE);

    return Lists.cartesianProduct(compressionCodecs, encodings, valueCounts, maxValues).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> floatEncodingsSource() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);
    final var encodings =
        List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY /*, Encoding.BYTE_STREAM_SPLIT*/);
    final var valueCounts = List.of(1, 2001);

    return Lists.cartesianProduct(compressionCodecs, encodings, valueCounts).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  private static Stream<Arguments> boolEncodingsSource() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);
    final var encodings = List.of(Encoding.PLAIN, Encoding.RLE, Encoding.RLE_DICTIONARY);
    final var valueCounts = List.of(1, 10_000);

    return Lists.cartesianProduct(compressionCodecs, encodings, valueCounts).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  static final String RANDOM_STRING_CHARACTERS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

  private String randomString(final Random random) {
    final var length = random.nextInt(34);
    char[] str = new char[length];
    for (var i = 0; i < length; i++) {
      str[i] = RANDOM_STRING_CHARACTERS.charAt(random.nextInt(RANDOM_STRING_CHARACTERS.length()));
    }
    return new String(str);
  }

  @ParameterizedTest
  @MethodSource("byteEncodingsSource")
  public void byteEncodingsRoundTrip(
      final CompressionCodec codec, final Encoding encoding, final int valueCount)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
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
          return Example.newBuilder().setSomeString(randomString(random)).build();
        });
  }

  @ParameterizedTest
  @MethodSource("intEncodingsSource")
  public void intEncodingsRoundTrip(
      final CompressionCodec codec,
      final Encoding encoding,
      final int valueCount,
      final int maxValue)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
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
  @MethodSource("floatEncodingsSource")
  public void floatEncodingsRoundTrip(
      final CompressionCodec codec, final Encoding encoding, final int valueCount)
      throws Exception {
    final var random = new Random();

    encodingRoundTrip(
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

  //  @ParameterizedTest
  //  @MethodSource("boolEncodingsSource")
  //  public void boolEncodingsRoundTrip(
  //      final CompressionCodec codec, final Encoding encoding, final int valueCount)
  //      throws Exception {
  //    final var random = new Random();
  //
  //    encodingRoundTrip(
  //        codec,
  //        (columnMetaData, distinctValues, totalValues, totalNulls) -> {
  //          if (columnMetaData.path_in_schema.size() == 1
  //              && columnMetaData.path_in_schema.get(0).equals("some_bool")) {
  //            return encoding;
  //          }
  //          return Encoding.PLAIN;
  //        },
  //        valueCount,
  //        () -> {
  //          return Example.newBuilder().setSomeBool(random.nextBoolean()).build();
  //        });
  //  }

  private void encodingRoundTrip(
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
                Example.getDescriptor(), new ProtobufParquetConfig(false)))) {
      writer.write(expectedProtobufs.iterator());
    }

    outputStream.writeTo(
        Files.newOutputStream(
            Path.of("/tmp/roundtrip.encoding.protobuf." + compressionCodec + ".parquet")));
    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      final var footer = ParquetFooter.read(byteRangeReader).join();
      final var schema = ParquetSchemaNode.from(footer.schema);

      var rowIndex = 0;
      for (RowGroup rowGroup : footer.row_groups) {
        final var rowGroupReader = new RowGroupReader(rowGroup, schema);
        final var rowIterator =
            rowGroupReader.getRowIterator(
                new RowReadSpec<>(new ProtobufReader<>(Example::newBuilder, schema)),
                byteRangeReader);
        while (rowIterator.hasNext()) {
          final var row = rowIterator.next();
          final var expectedProtobuf = expectedProtobufs.get(rowIndex);
          assertEquals(expectedProtobuf, row, "Row " + rowIndex + " did not match");
          rowIndex++;
        }
      }
      assertEquals(expectedProtobufs.size(), rowIndex, "Row count did not match");
    }
  }
}
