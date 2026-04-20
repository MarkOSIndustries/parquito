package com.markosindustries.parquito;

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
import java.util.List;
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
  public void writeProtobufsThenReadThem(CompressionCodec compressionCodec) throws Exception {
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

    final var outputStream = new ByteBufferOutputStream();
    try (final var writer =
        new RowGroupWriter<>(
            outputStream,
            new RowGroupWriter.WriteSpec(
                2, compressionCodec
                /* TODO - config for where to keep unique values? eg: disk/heap*/ ),
            ProtobufWriter.<Example>fromDescriptor(
                Example.getDescriptor(), new ProtobufParquetConfig(true)))) {
      writer.write(inputProtobufs.iterator());
    }

    outputStream.writeTo(
        Files.newOutputStream(Path.of("/tmp/roundtrip.protobuf." + compressionCodec + ".parquet")));
    final var parquetFileBuffer = outputStream.asByteBuffer();
    try (final var byteRangeReader = new ByteBufferByteRangeReader(parquetFileBuffer)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new ProtobufReader<>(Example::newBuilder, schema)),
                          byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var row = rowIterator.next();
                    System.err.println(row.toString());
                  }
                }
              })
          .join();
    }
  }
}
