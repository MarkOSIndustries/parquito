package com.markosindustries.parquito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.protobuf.ProtobufParquetConfig;
import com.markosindustries.parquito.protobuf.ProtobufReader;
import com.markosindustries.parquito.protobuf.ProtobufSchemaConverter;
import com.markosindustries.parquito.protobuf.ProtobufWriter;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.parquet.format.CompressionCodec;
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
                Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build()))) {
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
                    (type, schemaPath, distinctValues, totalValues, totalNulls) ->
                        schemaPath.path[schemaPath.path.length - 1].name.contains("some_string")
                            ? Optional.of(0.00001)
                            : Optional.empty())
                .build(),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build()))) {
      writer.write(expectedProtobufs.iterator());
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

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canUseExcludeOnFieldsDuringWrite(CompressionCodec compressionCodec) throws Exception {
    schemaTraversalSpecOnWriteTest(
        compressionCodec,
        inputRow -> inputRow.toBuilder().clearSomeChild().clearSomeBool().build(),
        fullSchema ->
            SchemaTraversalSpecs.excludeAll(
                fullSchema.parseDotSeparatedPath("some_child"),
                fullSchema.parseDotSeparatedPath("some_bool")));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canUseIncludeOnFieldsDuringWrite(CompressionCodec compressionCodec) throws Exception {
    schemaTraversalSpecOnWriteTest(
        compressionCodec,
        inputRow -> {
          final var result = Example.newBuilder().setSomeBool(inputRow.getSomeBool());
          if (inputRow.hasSomeChild()) {
            result.setSomeChild(inputRow.getSomeChild().toBuilder());
          }
          return result.build();
        },
        fullSchema ->
            SchemaTraversalSpecs.includeAll(
                fullSchema.parseDotSeparatedPath("some_child"),
                fullSchema.parseDotSeparatedPath("some_bool")));
  }

  public void schemaTraversalSpecOnWriteTest(
      final CompressionCodec compressionCodec,
      final Function<Example, Example> expectedRowModification,
      final Function<ParquetSchemaNode.Root, SchemaTraversalSpec> makeSchemaTraversalSpec)
      throws Exception {
    final var inputProtobufs =
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

    final var expectedProtobufs = inputProtobufs.stream().map(expectedRowModification).toList();

    final var outputStream = new ByteBufferOutputStream();
    final var protobufSchemaConverter =
        new ProtobufSchemaConverter(ProtobufParquetConfig.newBuilder().build());
    final var fullSchema =
        protobufSchemaConverter.convertDescriptorToSchema(Example.getDescriptor());
    final var writeSchema = fullSchema.trim(makeSchemaTraversalSpec.apply(fullSchema));
    final var protobufWriter = new ProtobufWriter<Example>(Example.getDescriptor(), writeSchema);
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder()
                .withTargetBytesPerRowGroup(10)
                .withCompressionCodec(compressionCodec)
                .withBloomFilterSelector(
                    (type, schemaPath, distinctValues, totalValues, totalNulls) ->
                        schemaPath.path[schemaPath.path.length - 1].name.contains("some_string")
                            ? Optional.of(0.00001)
                            : Optional.empty())
                .build(),
            protobufWriter)) {
      writer.write(inputProtobufs.iterator());
    }

    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var readSchema = ParquetSchemaNode.from(footer.schema);

                var rowIndex = 0;
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, readSchema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new ProtobufReader<>(Example::newBuilder, readSchema)),
                          byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var row = rowIterator.next();
                    final var expectedProtobuf = expectedProtobufs.get(rowIndex);
                    assertEquals(expectedProtobuf, row, "Row " + rowIndex + " did not match");
                    rowIndex++;
                  }
                }
                assertEquals(expectedProtobufs.size(), rowIndex, "Row count did not match");
              })
          .join();
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canUseExcludeOnFieldsDuringRead(CompressionCodec compressionCodec) throws Exception {
    schemaTraversalSpecOnReadTest(
        compressionCodec,
        inputRow -> inputRow.toBuilder().clearSomeChild().clearSomeBool().build(),
        fullSchema ->
            SchemaTraversalSpecs.excludeAll(
                fullSchema.parseDotSeparatedPath("some_child"),
                fullSchema.parseDotSeparatedPath("some_bool")));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canUseIncludeOnFieldsDuringRead(CompressionCodec compressionCodec) throws Exception {
    schemaTraversalSpecOnReadTest(
        compressionCodec,
        inputRow -> {
          final var result = Example.newBuilder().setSomeBool(inputRow.getSomeBool());
          if (inputRow.hasSomeChild()) {
            result.setSomeChild(inputRow.getSomeChild().toBuilder());
          }
          return result.build();
        },
        fullSchema ->
            SchemaTraversalSpecs.includeAll(
                fullSchema.parseDotSeparatedPath("some_child"),
                fullSchema.parseDotSeparatedPath("some_bool")));
  }

  public void schemaTraversalSpecOnReadTest(
      final CompressionCodec compressionCodec,
      final Function<Example, Example> expectedRowModification,
      final Function<ParquetSchemaNode.Root, SchemaTraversalSpec> makeSchemaTraversalSpec)
      throws Exception {
    final var inputProtobufs =
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

    final var expectedProtobufs = inputProtobufs.stream().map(expectedRowModification).toList();

    final var outputStream = new ByteBufferOutputStream();
    final var protobufSchemaConverter =
        new ProtobufSchemaConverter(ProtobufParquetConfig.newBuilder().build());
    final var writeSchema =
        protobufSchemaConverter.convertDescriptorToSchema(Example.getDescriptor());
    final var protobufWriter = new ProtobufWriter<Example>(Example.getDescriptor(), writeSchema);
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder()
                .withTargetBytesPerRowGroup(10)
                .withCompressionCodec(compressionCodec)
                .withBloomFilterSelector(
                    (type, schemaPath, distinctValues, totalValues, totalNulls) ->
                        schemaPath.path[schemaPath.path.length - 1].name.contains("some_string")
                            ? Optional.of(0.00001)
                            : Optional.empty())
                .build(),
            protobufWriter)) {
      writer.write(inputProtobufs.iterator());
    }

    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var readSchema = ParquetSchemaNode.from(footer.schema);
                var rowIndex = 0;
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, readSchema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(
                              new ProtobufReader<>(Example::newBuilder, readSchema),
                              makeSchemaTraversalSpec.apply(readSchema)),
                          byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var row = rowIterator.next();
                    final var expectedProtobuf = expectedProtobufs.get(rowIndex);
                    assertEquals(expectedProtobuf, row, "Row " + rowIndex + " did not match");
                    rowIndex++;
                  }
                }
                assertEquals(expectedProtobufs.size(), rowIndex, "Row count did not match");
              })
          .join();
    }
  }
}
