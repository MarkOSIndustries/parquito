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
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
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
                System.out.println("Oh my v " + footer.version + " c " + footer.created_by);
                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  final var rowIterator =
                      rowGroupReader.getRowIterator(new MapReader(), schema, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final var next = rowIterator.next();
                    Assertions.assertTrue(next.containsKey("some_repeated"));
                    Assertions.assertTrue(next.containsKey("some_string"));
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
                      rowGroupReader.getRowIterator(new JSONReader(), schema, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final JSONObject next = rowIterator.next();
                    Assertions.assertEquals(
                        "{\"some_repeated\":[],\"some_string\":\"\"}", next.toString());
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
                          new ProtobufReader(Example::newBuilder), schema, byteRangeReader);
                  var rows = 0;
                  while (rowIterator.hasNext()) {
                    final Example next = (Example) rowIterator.next();
                    Assertions.assertEquals(expectedProtobufs.get(rows), next);
                    rows++;
                  }
                  Assertions.assertEquals(expectedProtobufs.size(), rows);
                }
              })
          .join();
    }
  }

  private static File generateFileUsingApacheHadoop(
      List<Example> rows,
      CompressionCodecName codecName,
      final ParquetProperties.WriterVersion writerVersion)
      throws IOException {
    final File tempFile = File.createTempFile("integration-test", ".parquet");
    tempFile.deleteOnExit();

    try (final ParquetWriter<Example> writer =
        ProtoParquetWriter.<Example>builder(new SimpleOutputFile(tempFile))
            .withMessage(Example.class)
            .withCompressionCodec(codecName)
            .withWriteMode(OVERWRITE)
            .withWriterVersion(writerVersion)
            .build()) {
      for (Example row : rows) {
        writer.write(row);
      }
    }

    return tempFile;
  }

  public void randomJunkForFutureTests() throws IOException {
    final var file =
        generateFileUsingApacheHadoop(
            List.of(Example.newBuilder().build(), Example.newBuilder().build()),
            CompressionCodecName.SNAPPY,
            ParquetProperties.WriterVersion.PARQUET_1_0);
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      ParquetFooter.read(byteRangeReader)
          .thenAccept(
              footer -> {
                final var schema = ParquetSchemaNode.from(footer.schema);

                for (RowGroup rowGroup : footer.row_groups) {
                  final var rowGroupReader = new RowGroupReader(rowGroup);
                  //                  final var ck =
                  //                      rowGroupReader.getColumnChunkForSchemaPath(
                  //                          "message", "contact_id", "cs_shard_contact_id",
                  // "contact_id");
                  //                  final var columnType = ColumnType.create(ck.get(), schema);
                  //                  final var columnChunk = ColumnChunk.create(ck.get(),
                  // columnType, byteRangeReader);
                  //                  columnChunk.mightContain("");

                  final var rowIterator =
                      rowGroupReader.getRowIterator(new JSONReader(), schema, byteRangeReader);
                  var rows = 0L;
                  while (rowIterator.hasNext()) {
                    final JSONObject next = rowIterator.next();
                    rows++;
                  }
                  Assertions.assertEquals(2, rows);

                  //          for (org.apache.parquet.format.ColumnChunk columnChunkHeader :
                  // rowGroup.columns) {
                  //            if(columnChunkHeader.file_path != null) {
                  //              throw new RuntimeException("Sorry, can't handle chunks in
                  // different files yet");
                  //            }
                  //            if(columnChunkHeader.meta_data.total_compressed_size >
                  // Integer.MAX_VALUE) {
                  //              throw new RuntimeException("Sorry, can't handle chunks bigger than
                  // " + Integer.MAX_VALUE + " bytes yet");
                  //            }
                  //
                  //            final var columnType = ColumnType.create(columnChunkHeader, schema);
                  //            var columnChunk = ColumnChunk.create(columnChunkHeader, columnType,
                  // byteRangeReader);
                  //            System.out.println(columnChunk + " at " +
                  // Math.min(columnChunk.getHeader().file_offset,
                  // columnChunk.getHeader().meta_data.dictionary_page_offset) + " in row group at "
                  // + rowGroup.file_offset);
                  //
                  //
                  //            System.out.println(columnChunk + " -- stats test " +
                  // columnChunk.mightContain("0A070A0132108FC93D"));
                  //            if(columnChunk.hasRangeStats()) {
                  //              System.out.println(columnChunk + " -- stats " +
                  // columnChunk.getStatsMin() + " -> " + columnChunk.getStatsMax());
                  //            }
                  //
                  //            // TODO - if we decide based on stats etc, then we run this
                  //
                  // columnChunk.readPages(byteRangeReader).join().forEachRemaining(dataPage -> {
                  //              System.out.println(columnChunk + " -- page with " +
                  // dataPage.getNonNullValues() + " values");
                  //              for (int i = 0; i < dataPage.getNonNullValues(); i++) {
                  //                dataPage.getValue(i);
                  //                //                System.out.println("    value " +
                  // dataPage.getValue(i));
                  //              }
                  //            });
                  //          }
                }
              })
          .join();
    }
  }
}
