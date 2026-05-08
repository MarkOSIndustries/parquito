package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquitoAsReaderCompatibilityTests.generateFileUsingApacheHadoop;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.filesys.SimpleInputFile;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ParquetRewriterTests {
  private static Stream<Arguments> writerConfigCombinations() {
    final var compressionCodecs =
        List.of(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP);
    final var writerVersions =
        List.of(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            ParquetProperties.WriterVersion.PARQUET_2_0);
    final var specsComplaint = List.of(true, false);

    return Lists.cartesianProduct(compressionCodecs, writerVersions, specsComplaint).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsProtobuf(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws IOException {
    final var originalProtobufs =
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
                .putAllSomeMap(
                    Map.of(
                        123L,
                        ExampleChild.newBuilder()
                            .setSomeString("strx")
                            .addAllSomeStrings(List.of("str1", "str2"))
                            .build(),
                        456L,
                        ExampleChild.newBuilder()
                            .setSomeString("stry")
                            .addAllSomeStrings(List.of("str1", "str2"))
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

    final var expectedProtobufs =
        originalProtobufs.stream().filter(r -> r.getSomeString().equals("stra")).toList();

    final var file =
        generateFileUsingApacheHadoop(
            originalProtobufs,
            codecName,
            writerVersion,
            parquetSpecsCompliant,
            List.of(),
            List.of());

    final var rewriter =
        new ParquetRewriter(
            rowGroupReader ->
                ParquetPredicates.equals(
                    rowGroupReader,
                    "stra",
                    rowGroupReader.schemaRoot().parseDotSeparatedPath("some_string")));
    final File rewrittenFile = File.createTempFile("rewrite-test", ".parquet");
    rewrittenFile.deleteOnExit();
    try (final var byteRangeReader = new FileByteRangeReader(file);
        final var outputStream = Files.newOutputStream(rewrittenFile.toPath())) {
      rewriter
          .rewrite(
              byteRangeReader,
              outputStream,
              schema ->
                  WriteSpec.newBuilder()
                      .withCompressionCodec(CompressionCodec.SNAPPY)
                      .withBloomFilterSelector(
                          BloomFilterSelector.fpp(
                              Map.of(schema.parseDotSeparatedPath("some_string"), 0.001)))
                      .build())
          .join();
    }

    try (final var reader =
        ProtoParquetReader.<Example.Builder>builder(new SimpleInputFile(rewrittenFile)).build()) {
      int rowIndex = 0;
      for (var builder = reader.read(); builder != null; builder = reader.read()) {
        assertEquals(expectedProtobufs.get(rowIndex), builder.build());
        rowIndex++;
      }
      assertEquals(expectedProtobufs.size(), rowIndex);
    }
  }
}
