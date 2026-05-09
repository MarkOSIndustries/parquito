package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;

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
        IntEncodings.INT_ENCODING_DICTIONARY_INDICES.decode(
            expectedValues, bitWidth, decompressedPageStream);

    return new Values<ReadAs>() {
      @Override
      public ReadAs get(final int index) {
        return columnChunkReader.getDictionaryPage().getValues().get(dictionaryIndices[index]);
      }

      @Override
      public int count() {
        return expectedValues;
      }

      @Override
      public PredicateMaterialisedMatches materialise(final ColumnPredicate<ReadAs, ?> predicate) {
        final var dictionaryPage = columnChunkReader.getDictionaryPage();
        final var dictionaryPageValues = dictionaryPage.getValues();

        final var matchingDictionaryIndices = new BitSet(dictionaryPage.getTotalValues());
        for (var dictionaryIndex = 0;
            dictionaryIndex < dictionaryPage.getTotalValues();
            dictionaryIndex++) {
          if (predicate.valueMatches(dictionaryPageValues.get(dictionaryIndex))) {
            matchingDictionaryIndices.set(dictionaryIndex);
          }
        }

        return index -> matchingDictionaryIndices.get(dictionaryIndices[index]);
      }
    };
  }

  @Override
  public void encode(
      final FastDictionary<ReadAs, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    var maxDictionaryIndex = 0;
    for (var i = 0; i < values.length(); i++) {
      if (values.getIndex(i) > maxDictionaryIndex) {
        maxDictionaryIndex = values.getIndex(i);
      }
    }
    final var bitWidth = Maths.bitWidth(maxDictionaryIndex);

    uncompressedPageStream.write(bitWidth);
    IntEncodings.INT_ENCODING_DICTIONARY_INDICES.encode(
        values.getIndices(), bitWidth, uncompressedPageStream);
  }

  @Override
  public int refineBytesRequiredEstimate(
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<ReadAs> columnChunkWriter) {
    return Math.ceilDiv(Maths.bitWidth(valueCount) * valueCount, 8);
  }
}
