package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.format.PageHeader;

public class DataPageV1<ReadAs extends Comparable<ReadAs>> implements DataPage<ReadAs> {
  private final int[] repetitionLevels;
  private final int[] definitionLevels;
  private final PageHeader pageHeader;
  private final int totalValues;
  private final int nonNullValues;
  private final Values<ReadAs> values;

  protected DataPageV1(
      final PageHeader pageHeader,
      final ColumnChunkReader<ReadAs> columnChunkReader,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var decompressedPageStream =
        CompressionCodecs.decompress(
            columnChunkReader.getHeader().meta_data.codec, new ByteBufferInputStream(pageBuffer));

    this.repetitionLevels =
        IntEncodings.getDecoder(pageHeader.data_page_header.repetition_level_encoding)
            .decode(
                pageHeader.data_page_header.num_values,
                IntEncodings.bitWidth(
                    columnChunkReader.getColumnType().schemaNode().getRepetitionLevelMax()),
                decompressedPageStream);
    this.definitionLevels =
        IntEncodings.getDecoder(pageHeader.data_page_header.definition_level_encoding)
            .decode(
                pageHeader.data_page_header.num_values,
                IntEncodings.bitWidth(
                    columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax()),
                decompressedPageStream);
    this.totalValues = pageHeader.data_page_header.num_values;
    this.nonNullValues =
        (int)
            Arrays.stream(definitionLevels)
                .filter(d -> d == columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax())
                .count();
    this.values =
        Encodings.<ReadAs>getDecoder(pageHeader.data_page_header.encoding)
            .decode(
                nonNullValues,
                pageHeader.uncompressed_page_size,
                decompressedPageStream,
                columnChunkReader);
  }

  @Override
  public int[] getRepetitionLevels() {
    return repetitionLevels;
  }

  @Override
  public int[] getDefinitionLevels() {
    return definitionLevels;
  }

  @Override
  public PageHeader getPageHeader() {
    return pageHeader;
  }

  @Override
  public int getTotalValues() {
    return totalValues;
  }

  @Override
  public int getNonNullValues() {
    return nonNullValues;
  }

  @Override
  public ReadAs getValue(final int index) {
    return values.get(index);
  }
}
