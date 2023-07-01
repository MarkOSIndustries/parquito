package com.markosindustries.parquito;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

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
import java.util.stream.Stream;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ParquetCompatibilityTests {
  private static Stream<Arguments> writerConfigCombinations() {
    return Stream.of(
        Arguments.of(
            CompressionCodecName.UNCOMPRESSED, ParquetProperties.WriterVersion.PARQUET_1_0),
        Arguments.of(CompressionCodecName.SNAPPY, ParquetProperties.WriterVersion.PARQUET_1_0),
        Arguments.of(CompressionCodecName.GZIP, ParquetProperties.WriterVersion.PARQUET_1_0),
        Arguments.of(
            CompressionCodecName.UNCOMPRESSED, ParquetProperties.WriterVersion.PARQUET_2_0),
        Arguments.of(CompressionCodecName.SNAPPY, ParquetProperties.WriterVersion.PARQUET_2_0),
        Arguments.of(CompressionCodecName.GZIP, ParquetProperties.WriterVersion.PARQUET_2_0));
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsMap(
      CompressionCodecName codecName, ParquetProperties.WriterVersion writerVersion)
      throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(Example.newBuilder().build(), Example.newBuilder().build()),
            codecName,
            writerVersion);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new MapReader()), schema, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final var next = rowIterator.next();
                    Assertions.assertTrue(next.containsKey("some_repeated"));
                    Assertions.assertTrue(next.containsKey("some_string"));
                    Assertions.assertTrue(next.containsKey("some_map"));
                    rows++;
                  }
                  Assertions.assertEquals(2, rows);
                }
              })
          .join();
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsJson(
      CompressionCodecName codecName, ParquetProperties.WriterVersion writerVersion)
      throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(Example.newBuilder().build(), Example.newBuilder().build()),
            codecName,
            writerVersion);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new JSONReader()), schema, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final JSONObject next = rowIterator.next();
                    Assertions.assertEquals(
                        "{\"some_map\":[],\"some_repeated\":[],\"some_string\":\"\"}",
                        next.toString());
                    rows++;
                  }
                  Assertions.assertEquals(2, rows);
                }
              })
          .join();
    }
  }

  @ParameterizedTest
  @MethodSource("writerConfigCombinations")
  public void canReadAFileAsProtobuf(
      CompressionCodecName codecName, ParquetProperties.WriterVersion writerVersion)
      throws IOException {
    final var expectedProtobufs =
        List.of(
            Example.newBuilder()
                .setSomeChild(
                    ExampleChild.newBuilder()
                        .setSomeString("str")
                        .addAllSomeStrings(List.of("str1", "str2"))
                        .setSomeInt32(Integer.MAX_VALUE - 872634)
                        .setSomeInt64(Integer.MAX_VALUE + 872634L)
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
                        .setSomeString("str")
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
                        .setSomeString("str")
                        .addAllSomeStrings(List.of("str1", "str2"))
                        .setSomeInt32(Integer.MAX_VALUE - 872634)
                        .setSomeInt64(Integer.MAX_VALUE + 872634L)
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

    final var file = generateFileUsingApacheHadoop(expectedProtobufs, codecName, writerVersion);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(
                          new RowReadSpec<>(new ProtobufReader<Example>(Example::newBuilder)),
                          schema,
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
            ParquetProperties.WriterVersion.PARQUET_1_0);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var columnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(byteRangeReader, schema, "some_string")
                          .orElseThrow();
                  Assertions.assertTrue(columnChunkReader.mightContainObject("str"));
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
            "some_string");
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var columnChunkReader =
                      rowGroupReader
                          .getColumnChunkReaderForSchemaPath(byteRangeReader, schema, "some_string")
                          .orElseThrow();
                  Assertions.assertFalse(columnChunkReader.mightContainObject("str"));
                  Assertions.assertTrue(columnChunkReader.mightContainObject("stonks"));
                }
              })
          .join();
    }
  }

  private static File generateFileUsingApacheHadoop(
      List<Example> rows,
      CompressionCodecName codecName,
      final ParquetProperties.WriterVersion writerVersion,
      String... dictionaryColumnPaths)
      throws IOException {
    final File tempFile = File.createTempFile("integration-test", ".parquet");
    tempFile.deleteOnExit();

    final var writerBuilder =
        ProtoParquetWriter.<Example>builder(new SimpleOutputFile(tempFile))
            .withMessage(Example.class)
            .withCompressionCodec(codecName)
            .withWriteMode(OVERWRITE)
            .withWriterVersion(writerVersion);
    for (final String dictionaryColumnPath : dictionaryColumnPaths) {
      writerBuilder.withDictionaryEncoding(dictionaryColumnPath, true);
    }

    try (final var writer = writerBuilder.build()) {
      for (Example row : rows) {
        writer.write(row);
      }
    }

    return tempFile;
  }
}
