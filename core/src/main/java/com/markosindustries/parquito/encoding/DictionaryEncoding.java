package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;

public class DictionaryEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    final var bitWidth = decompressedPageStream.read();

    final var dictionaryIndices =
        IntEncodings.INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.decode(
            expectedValues, bitWidth, decompressedPageStream);

    return (idx) -> columnChunkReader.getDictionaryPage().getValue(dictionaryIndices[idx]);
  }
}
