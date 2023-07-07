package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ParquetPredicate;
import com.markosindustries.parquito.page.PredicateMatcher;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    return new Values<ReadAs>() {
      @Override
      public ReadAs get(final int index) {
        return columnChunkReader.getDictionaryPage().getValues().get(dictionaryIndices[index]);
      }

      @Override
      public PredicateMatcher matcher(final ParquetPredicate<ReadAs> predicate) {
        final var dictionaryPage = columnChunkReader.getDictionaryPage();
        final var dictionaryPageValues = dictionaryPage.getValues();
        final Set<Integer> matchingDictionaryIndices =
            IntStream.range(0, dictionaryPage.getTotalValues())
                .filter(
                    dictionaryIndex ->
                        predicate.valueMatches(dictionaryPageValues.get(dictionaryIndex)))
                .boxed()
                .collect(Collectors.toSet());

        return index -> matchingDictionaryIndices.contains(dictionaryIndices[index]);
      }
    };
  }
}
