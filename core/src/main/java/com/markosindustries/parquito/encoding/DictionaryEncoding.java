package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.LazyBitSet;
import com.markosindustries.parquito.ParquetIOException;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DictionaryEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var bitWidth = decompressedPageBuffer.get();

    final var dictionaryIndices =
        IntEncodings.INT_ENCODING_DICTIONARY_INDICES.decode(
            expectedValues, bitWidth, decompressedPageBuffer);

    if (dictionaryIndices.length != expectedValues) {
      throw new ParquetIOException(
          "Unexpected dictionary value count - expected "
              + expectedValues
              + " but found "
              + dictionaryIndices.length);
    }

    return new Values() {
      @Override
      public boolean getBoolean(final int index) {
        return columnChunkReader
            .getDictionaryPage()
            .getValues()
            .getBoolean(dictionaryIndices[index]);
      }

      @Override
      public ByteBuffer getByteBuffer(final int index) {
        return columnChunkReader
            .getDictionaryPage()
            .getValues()
            .getByteBuffer(dictionaryIndices[index]);
      }

      @Override
      public double getDouble(final int index) {
        return columnChunkReader
            .getDictionaryPage()
            .getValues()
            .getDouble(dictionaryIndices[index]);
      }

      @Override
      public float getFloat(final int index) {
        return columnChunkReader.getDictionaryPage().getValues().getFloat(dictionaryIndices[index]);
      }

      @Override
      public int getInt32(final int index) {
        return columnChunkReader.getDictionaryPage().getValues().getInt32(dictionaryIndices[index]);
      }

      @Override
      public long getInt64(final int index) {
        return columnChunkReader.getDictionaryPage().getValues().getInt64(dictionaryIndices[index]);
      }

      @Override
      public int count() {
        return expectedValues;
      }

      @Override
      public <T> PredicateMaterialisedMatches materialise(final ColumnPredicate<T, ?> predicate) {
        final var dictionaryPage = columnChunkReader.getDictionaryPage();
        final var dictionaryPageValues = dictionaryPage.getValues();
        final var matchingDictionaryIndices =
            new LazyBitSet(
                dictionaryPage.getTotalValues(),
                dictionaryIndex -> predicate.valueMatches(dictionaryPageValues, dictionaryIndex));

        return index -> matchingDictionaryIndices.get(dictionaryIndices[index]);
      }
    };
  }

  @Override
  public void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException {
    final var indices = values.getIndices();

    var maxDictionaryIndex = 0;
    for (var i = 0; i < values.length(); i++) {
      if (indices.getInt(i) > maxDictionaryIndex) {
        maxDictionaryIndex = indices.getInt(i);
      }
    }
    final var bitWidth = Maths.bitWidth(maxDictionaryIndex);

    uncompressedPageStream.write(bitWidth);
    IntEncodings.INT_ENCODING_DICTIONARY_INDICES.encode(indices, bitWidth, uncompressedPageStream);
  }

  @Override
  public int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return Maths.ceilDivPow2(Maths.bitWidth(values.length()) * values.length(), 3);
  }
}
