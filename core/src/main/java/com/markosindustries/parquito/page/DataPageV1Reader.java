package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.ParquetIOException;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.encoding.Maths;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.format.PageHeader;

public class DataPageV1Reader implements DataPageReader {
  private final int[] repetitionLevels;
  private final int[] definitionLevels;
  private final PageHeader pageHeader;
  private final int totalValues;
  private final int nonNullValues;
  private final Values values;

  protected DataPageV1Reader(
      final PageHeader pageHeader,
      final ColumnChunkReader columnChunkReader,
      final ByteBuffer pageBuffer)
      throws IOException {
    this.pageHeader = pageHeader;

    final var decompressedPageBuffer =
        CompressionCodecs.decompress(columnChunkReader.getHeader().meta_data.codec, pageBuffer);
    if (decompressedPageBuffer.remaining() < pageHeader.uncompressed_page_size) {
      throw new ParquetIOException(
          "There are insufficient decompressed bytes to read the data page - need "
              + pageHeader.uncompressed_page_size
              + " but got "
              + decompressedPageBuffer.remaining());
    }
    this.repetitionLevels =
        IntEncodings.getDecoder(pageHeader.data_page_header.repetition_level_encoding)
            .decode(
                pageHeader.data_page_header.num_values,
                Maths.bitWidth(
                    columnChunkReader.getColumnType().schemaNode().getRepetitionLevelMax()),
                decompressedPageBuffer);
    this.definitionLevels =
        IntEncodings.getDecoder(pageHeader.data_page_header.definition_level_encoding)
            .decode(
                pageHeader.data_page_header.num_values,
                Maths.bitWidth(
                    columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax()),
                decompressedPageBuffer);
    this.totalValues = pageHeader.data_page_header.num_values;
    this.nonNullValues =
        (int)
            Arrays.stream(definitionLevels)
                .filter(
                    d ->
                        d == columnChunkReader.getColumnType().schemaNode().getDefinitionLevelMax())
                .count();
    this.values =
        Encodings.getEncoding(pageHeader.data_page_header.encoding)
            .decode(nonNullValues, decompressedPageBuffer.slice(), columnChunkReader);
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
  public Values getValues() {
    return values;
  }
}
