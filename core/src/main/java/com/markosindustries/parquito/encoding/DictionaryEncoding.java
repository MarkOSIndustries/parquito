package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetPredicate;
import com.markosindustries.parquito.page.PredicateMatcher;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.List;

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
        if (predicate instanceof ParquetPredicate.All<ReadAs>) {
          return index -> true;
        }

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
      final List<ReadAs> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    final var dictionaryPageWriter = columnChunkWriter.getDictionaryPageWriter();

    final var bitWidth = Maths.bitWidth(dictionaryPageWriter.getNumValues());

    uncompressedPageStream.write(bitWidth);
    IntEncodings.INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER.encode(
        dictionaryPageWriter.indexValues(values), bitWidth, uncompressedPageStream);
  }
}
