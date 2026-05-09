package com.markosindustries.parquito;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.filesys.SimpleOutputFile;
import com.markosindustries.parquito.json.JSONReader;
import com.markosindustries.parquito.protobuf.ProtobufReader;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests where parquito does the reading and the hadoop parquet library does the writing */
public class ParquitoAsReaderCompatibilityTests {
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
  public void canReadEmptyFile(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws Exception {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(), codecName, writerVersion, parquetSpecsCompliant, List.of(), List.of());
    final var actuallyReadTheFile = new AtomicBoolean(false);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                actuallyReadTheFile.set(true);
                final var schema = ParquetSchemaNode.from(footer.schema);
                var rows = 0;
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new MapReader(schema)), byteRangeReader);
                  while (rowIterator.hasNext()) {
                    final var next = rowIterator.next();
                    rows++;
                  }
                }
                Assertions.assertEquals(0, rows);
              })
          .join();
      assertTrue(actuallyReadTheFile.get(), "We never actually read the contents of the file");
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsMap(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(Example.newBuilder().build(), Example.newBuilder().build()),
            codecName,
            writerVersion,
            parquetSpecsCompliant,
            List.of(),
            List.of());
    final var actuallyReadTheFile = new AtomicBoolean(false);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                actuallyReadTheFile.set(true);
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new MapReader(schema)), byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final var next = rowIterator.next();
                    assertTrue(next.containsKey("some_repeated"));
                    assertTrue(next.containsKey("some_string"));
                    assertTrue(next.containsKey("some_map"));
                    rows++;
                  }
                  Assertions.assertEquals(2, rows);
                }
              })
          .join();
      assertTrue(actuallyReadTheFile.get(), "We never actually read the contents of the file");
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsJson(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(Example.newBuilder().build(), Example.newBuilder().build()),
            codecName,
            writerVersion,
            parquetSpecsCompliant,
            List.of(),
            List.of());
    // In specs compliant mode, the schema is able to represent the absense of a map or a list
    final var expectedJson =
        parquetSpecsCompliant
            ? "{\"some_string\":\"\",\"some_bool\":false}"
            : "{\"some_string\":\"\",\"some_repeated\":[],\"some_map\":[],\"some_bool\":false}";
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      final var actuallyReadTheFile = new AtomicBoolean(false);
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                actuallyReadTheFile.set(true);
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new JSONReader(schema)), byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final JSONObject next = rowIterator.next();
                    Assertions.assertEquals(expectedJson, next.toString());
                    rows++;
                  }
                  Assertions.assertEquals(2, rows);
                }
              })
          .join();
      assertTrue(actuallyReadTheFile.get(), "We never actually read the contents of the file");
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsProtobuf(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws IOException {
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

    final var file =
        generateFileUsingApacheHadoop(
            expectedProtobufs,
            codecName,
            writerVersion,
            parquetSpecsCompliant,
            List.of(),
            List.of());
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      final var actuallyReadTheFile = new AtomicBoolean(false);
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                actuallyReadTheFile.set(true);
                final var schema = ParquetSchemaNode.from(footer.schema);
                final var rowReadSpec =
                    new RowReadSpec<>(new ProtobufReader<Example>(Example::newBuilder, schema));
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);

                  final var rowIterator =
                      rowGroupReader.getRowIterator(rowReadSpec, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final Example next = rowIterator.next();
                    Assertions.assertEquals(expectedProtobufs.get(rows), next);
                    rows++;
                  }
                  Assertions.assertEquals(expectedProtobufs.size(), rows);
                }
              })
          .join();
      assertTrue(actuallyReadTheFile.get(), "We never actually read the contents of the file");
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canFilterUsingPredicatePushdown(
      CompressionCodecName codecName,
      ParquetProperties.WriterVersion writerVersion,
      boolean parquetSpecsCompliant)
      throws IOException {
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
    final var expectedProtobufs =
        inputProtobufs.stream()
            .filter(
                p ->
                    p.getSomeChild().getSomeInt32() == Integer.MAX_VALUE - 974456
                        && p.getSomeChild().getSomeString().compareTo("str") > 0)
            .toList();

    final var file =
        generateFileUsingApacheHadoop(
            inputProtobufs, codecName, writerVersion, parquetSpecsCompliant, List.of(), List.of());
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);

                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(
                              new ProtobufReader<Example>(Example::newBuilder, schema),
                              ParquetPredicates.intersection(
                                  ParquetPredicates.anyEquals(
                                      rowGroupReader,
                                      Integer.MAX_VALUE - 974456,
                                      schema.parsePathElements("some_child", "some_int32")),
                                  ParquetPredicates.anyGreaterThan(
                                      rowGroupReader,
                                      "str",
                                      schema.parsePathElements("some_child", "some_string")))),
                          byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final Example next = rowIterator.next();
                    Assertions.assertEquals(expectedProtobufs.get(rows), next);
                    rows++;
                  }
                  Assertions.assertEquals(expectedProtobufs.size(), rows);
                }
              })
          .join();
    }
  }

  @Test
  public void canCheckForValuePresenceUsingStats() throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(
                Example.newBuilder().setSomeString("styx").build(),
                Example.newBuilder().setSomeString("stab").build()),
            CompressionCodecName.SNAPPY,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            true,
            List.of(),
            List.of());
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var columnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(
                              byteRangeReader, schema.parseDotSeparatedPath("some_string"))
                          .orElseThrow();
                  assertTrue(columnChunkReader.mightContainObject("str"));
                  Assertions.assertFalse(columnChunkReader.mightContainObject("slab"));
                }
              })
          .join();
    }
  }

  @Test
  public void canCheckForValuePresenceUsingStatsAndDictionary() throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(
                Example.newBuilder().setSomeString("styx").build(),
                Example.newBuilder().setSomeString("stonks").build(),
                Example.newBuilder().setSomeString("styx").build(),
                Example.newBuilder().setSomeString("stab").build()),
            CompressionCodecName.SNAPPY,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            true,
            List.of("some_string"),
            List.of());
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var columnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(
                              byteRangeReader, schema.parseDotSeparatedPath("some_string"))
                          .orElseThrow();
                  Assertions.assertFalse(columnChunkReader.mightContainObject("str"));
                  assertTrue(columnChunkReader.mightContainObject("stonks"));
                  assertTrue(columnChunkReader.mightContainAnyObjects(List.of("str", "stonks")));
                  Assertions.assertFalse(
                      columnChunkReader.mightContainAnyObjects(List.of("str", "strut")));
                }
              })
          .join();
    }
  }

  @Test
  public void canCheckForValuePresenceUsingStatsAndBloomFilter() throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(
                Example.newBuilder().setSomeString("styx").build(),
                Example.newBuilder().setSomeString("stonks").build(),
                Example.newBuilder().setSomeString("styx").build(),
                Example.newBuilder().setSomeString("stab").build()),
            CompressionCodecName.SNAPPY,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            true,
            List.of(),
            List.of("some_string"));
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup, schema);
                  final var columnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(
                              byteRangeReader, schema.parseDotSeparatedPath("some_string"))
                          .orElseThrow();
                  Assertions.assertFalse(columnChunkReader.mightContainObject("str"));
                  assertTrue(columnChunkReader.mightContainObject("stonks"));
                  assertTrue(columnChunkReader.mightContainAnyObjects(List.of("str", "stonks")));
                  Assertions.assertFalse(
                      columnChunkReader.mightContainAnyObjects(List.of("str", "strut")));
                }
              })
          .join();
    }
  }

  static File generateFileUsingApacheHadoop(
      final List<Example> rows,
      final CompressionCodecName codecName,
      final ParquetProperties.WriterVersion writerVersion,
      final boolean parquetSpecsCompliant,
      final List<String> dictionaryColumnPaths,
      final List<String> bloomFilterColumnPaths)
      throws IOException {
    final File tempFile = File.createTempFile("integration-test", ".parquet");
    tempFile.deleteOnExit();

    final var conf = new Configuration();
    if (parquetSpecsCompliant) {
      ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    }

    final var writerBuilder =
        ProtoParquetWriter.<Example>builder(new SimpleOutputFile(tempFile))
            .withConf(conf)
            .withMessage(Example.class)
            .withCompressionCodec(codecName)
            .withWriteMode(OVERWRITE)
            .withWriterVersion(writerVersion);
    for (final String dictionaryColumnPath : dictionaryColumnPaths) {
      writerBuilder.withDictionaryEncoding(dictionaryColumnPath, true);
    }
    for (final String bloomFilterColumnPath : bloomFilterColumnPaths) {
      writerBuilder.withBloomFilterEnabled(bloomFilterColumnPath, true);
    }

    try (final var writer = writerBuilder.build()) {
      for (Example row : rows) {
        writer.write(row);
      }
    }

    return tempFile;
  }
}
