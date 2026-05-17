package com.markosindustries.parquito;

import static org.apache.parquet.proto.ProtoReadSupport.PB_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.filesys.SimpleInputFile;
import com.markosindustries.parquito.protobuf.ProtobufParquetConfig;
import com.markosindustries.parquito.protobuf.ProtobufWriter;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.proto.ProtoParquetReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests where parquito does the writing and the hadoop parquet library does the reading */
public class ParquitoAsWriterCompatibilityTests {
  private static Stream<Arguments> writerConfigCombinations() {
    final var compressionCodecs =
        List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP);

    return Lists.cartesianProduct(compressionCodecs).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canWriteEmptyFile(final CompressionCodec compressionCodec) throws Exception {
    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder().withCompressionCodec(compressionCodec).build(),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build()))) {
      writer.putMetaData(PB_CLASS, Example.class.getName());
      writer.write(Collections.emptyIterator());
    }

    final var tempFile = Files.createTempFile("parquito.as.writer." + compressionCodec, ".parquet");

    try (final var fileOutputStream = new FileOutputStream(tempFile.toFile())) {
      outputStream.writeTo(fileOutputStream);
    }

    try (final var reader =
        ProtoParquetReader.<Example.Builder>builder(new SimpleInputFile(tempFile.toFile()))
            .build()) {
      int rowIndex = 0;
      for (var builder = reader.read(); builder != null; builder = reader.read()) {
        rowIndex++;
      }
      assertEquals(0, rowIndex);
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void writeProtobufsThenReadThem(final CompressionCodec compressionCodec) throws Exception {
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

    final var protobufWriter =
        ProtobufWriter.<Example>fromDescriptor(
            Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build());

    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            WriteSpec.newBuilder()
                .withTargetBytesPerRowGroup(10)
                .withCompressionCodec(compressionCodec)
                .withBloomFilterSelector(
                    BloomFilterSelector.fpp(
                        Map.of(
                            protobufWriter.getSchemaRoot().parseDotSeparatedPath("some_string"),
                            0.001)))
                .build(),
            protobufWriter)) {
      writer.putMetaData(PB_CLASS, Example.class.getName());
      writer.write(inputProtobufs.iterator());
    }

    final var tempFile = Files.createTempFile("parquito.as.writer", ".parquet");

    try (final var fileOutputStream = new FileOutputStream(tempFile.toFile())) {
      outputStream.writeTo(fileOutputStream);
    }

    final var reader =
        ProtoParquetReader.<Example.Builder>builder(new SimpleInputFile(tempFile.toFile())).build();
    var rowIndex = 0;
    for (var builder = reader.read(); builder != null; builder = reader.read()) {
      assertEquals(inputProtobufs.get(rowIndex), builder.build());
      rowIndex++;
    }
    assertEquals(inputProtobufs.size(), rowIndex);
  }
}
