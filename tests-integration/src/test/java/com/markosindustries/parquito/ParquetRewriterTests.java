package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquitoAsReaderCompatibilityTests.generateFileUsingApacheHadoop;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.markosindustries.parquito.filesys.SimpleInputFile;
import com.markosindustries.parquito.predicates.AnyInSet;
import com.markosindustries.parquito.protobuf.ProtobufParquetConfig;
import com.markosindustries.parquito.protobuf.ProtobufReader;
import com.markosindustries.parquito.protobuf.ProtobufSchemaConverter;
import com.markosindustries.parquito.protobuf.ProtobufWriter;
import com.markosindustries.parquito.schemas.Example;
import com.markosindustries.parquito.schemas.ExampleChild;
import com.markosindustries.parquito.schemas.ExampleEnum;
import com.markosindustries.parquito.types.ConversionStrategy;
import com.markosindustries.parquito.types.StringConverter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
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
                ParquetPredicates.anyEquals(
                    rowGroupReader,
                    "stra",
                    rowGroupReader.schemaRoot().parseDotSeparatedPath("some_string"),
                    ConversionStrategy.DEFAULT));
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

  /**
   * Row groups in which every row is kept are byte-copied by {@link
   * RowGroupWriter#injectForeignRowGroup}, which has to shift every offset in their metadata to the
   * new location. Rewriting an already-rewritten file is the case which catches any offset it
   * misses, because a rewrite drops rows and so moves later row groups earlier in the file.
   */
  @ParameterizedTest
  @MethodSource("dictionaryColumnCompressionCodecs")
  public void canRewriteAFileWhichHasAlreadyBeenRewritten(final CompressionCodec compressionCodec)
      throws Exception {
    final var rowsPerRowGroup = 2;
    final var originalRows =
        IntStream.range(0, 8)
            .mapToObj(index -> Example.newBuilder().setSomeString("row" + index).build())
            .toList();

    final var originalFile = writeWithParquito(originalRows, rowsPerRowGroup, compressionCodec);

    // Drop a row from the first row group only, so later row groups get byte-copied to a
    // smaller offset than they originally had
    final var onceRewritten = rewriteExcluding(originalFile, Set.of("row0"), compressionCodec);
    assertColumnChunkOffsetsAreConsistent(onceRewritten);
    assertEquals(7, readSomeStrings(onceRewritten).size());

    // ...and now the pass that used to fail with "Negative size"
    final var twiceRewritten = rewriteExcluding(onceRewritten, Set.of("row4"), compressionCodec);
    assertColumnChunkOffsetsAreConsistent(twiceRewritten);
    assertEquals(
        List.of("row1", "row2", "row3", "row5", "row6", "row7"), readSomeStrings(twiceRewritten));
  }

  private static Stream<Arguments> dictionaryColumnCompressionCodecs() {
    return Stream.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP)
        .map(Arguments::of);
  }

  /**
   * Every {@code ColumnChunk.file_offset} must agree with the page offsets in its own metadata -
   * readers derive the dictionary page size from the gap between the two, so a stale {@code
   * file_offset} gives a negative data page size.
   */
  private static void assertColumnChunkOffsetsAreConsistent(final File file) {
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      final var footer = ParquetFooter.read(byteRangeReader).join();
      var dictionaryPagesSeen = 0;
      for (var rowGroupIndex = 0; rowGroupIndex < footer.row_groups.size(); rowGroupIndex++) {
        for (final var column : footer.row_groups.get(rowGroupIndex).columns) {
          final var describe =
              "row group "
                  + rowGroupIndex
                  + " column "
                  + String.join(".", column.meta_data.path_in_schema);
          assertEquals(column.meta_data.data_page_offset, column.file_offset, describe);
          if (column.meta_data.isSetDictionary_page_offset()) {
            dictionaryPagesSeen++;
            assertTrue(
                column.meta_data.data_page_offset > column.meta_data.dictionary_page_offset,
                describe + " dictionary page should precede its data pages");
          }
        }
      }
      assertTrue(dictionaryPagesSeen > 0, "Expected at least one dictionary-encoded column chunk");
    }
  }

  private static File writeWithParquito(
      final List<Example> rows, final int rowsPerRowGroup, final CompressionCodec compressionCodec)
      throws Exception {
    final var schema =
        new ProtobufSchemaConverter(ProtobufParquetConfig.newBuilder().build())
            .convertDescriptorToSchema(Example.getDescriptor());
    final var file = File.createTempFile("rewrite-twice-original", ".parquet");
    file.deleteOnExit();

    try (final var outputStream = Files.newOutputStream(file.toPath());
         final var writer =
             new RowGroupWriter<>(
                 outputStream,
                 WriteSpec.newBuilder()
                     .withCompressionCodec(compressionCodec)
                     .withMaxRowsPerRowGroup(rowsPerRowGroup)
                     .withEncodingSelector(dictionaryEncodedSomeString(schema))
                     .build(),
                 ProtobufWriter.fromDescriptor(
                     Example.getDescriptor(), ProtobufParquetConfig.newBuilder().build()))) {
      writer.write(rows);
    }
    return file;
  }

  private static File rewriteExcluding(
      final File source,
      final Set<String> someStringsToDrop,
      final CompressionCodec compressionCodec)
      throws Exception {
    final var rewriter =
        new ParquetRewriter(
            rowGroupReader ->
                ParquetPredicates.not(
                    new AnyInSet<>(
                        ColumnValuesSet.from(StringConverter.VARIABLE_LENGTH, someStringsToDrop),
                        rowGroupReader.schemaRoot().parseDotSeparatedPath("some_string"))));

    final var destination = File.createTempFile("rewrite-twice-pass", ".parquet");
    destination.deleteOnExit();
    try (final var byteRangeReader = new FileByteRangeReader(source);
         final var outputStream = Files.newOutputStream(destination.toPath())) {
      rewriter
          .rewrite(
              byteRangeReader,
              outputStream,
              schema ->
                  WriteSpec.newBuilder()
                      .withCompressionCodec(compressionCodec)
                      .withEncodingSelector(dictionaryEncodedSomeString(schema))
                      .build())
          .join();
    }
    return destination;
  }

  private static EncodingSelector dictionaryEncodedSomeString(final ParquetSchemaNode.Root schema) {
    return new EncodingSelector.DefaultEncodingSelector(
        Map.of(schema.parseDotSeparatedPath("some_string"), Encoding.RLE_DICTIONARY));
  }

  private static List<String> readSomeStrings(final File file) {
    final var someStrings = new ArrayList<String>();
    try (final var byteRangeReader = new FileByteRangeReader(file)) {
      final var footer = ParquetFooter.read(byteRangeReader).join();
      final var schema = ParquetSchemaNode.from(footer.schema);
      for (final var rowGroup : footer.row_groups) {
        final var rowIterator =
            new RowGroupReader(rowGroup, schema)
                .getRowIterator(
                    new RowReadSpec<>(new ProtobufReader<Example>(Example::newBuilder, schema)),
                    byteRangeReader);
        while (rowIterator.hasNext()) {
          someStrings.add(rowIterator.next().getSomeString());
        }
      }
    }
    return someStrings;
  }
}
