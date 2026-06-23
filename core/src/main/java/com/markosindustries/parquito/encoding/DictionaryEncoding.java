package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class DictionaryEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var bitWidth = decompressedPageStream.read();

    final var dictionaryIndices =
        IntEncodings.INT_ENCODING_DICTIONARY_INDICES.decode(
            expectedValues, bitWidth, decompressedPageStream);

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        columnChunkReader
            .getDictionaryPage()
            .getValues()
            .visit(pageIndex, dictionaryIndices[valueIndex], visitor);
      }

      @Override
      public int count() {
        return expectedValues;
      }

      @Override
      public <T> PredicateMaterialisedMatches materialise(
          final ColumnPredicate<T, ?> predicate, final Class<T> tClass) {
        final var dictionaryPage = columnChunkReader.getDictionaryPage();
        final var dictionaryPageValues = dictionaryPage.getValues();

        final var matchingDictionaryIndices = new BitSet(dictionaryPage.getTotalValues());

        final var predicateVisitor =
            new Visitor() {
              @Override
              public void visit(int pageIndex, final boolean value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visit(int pageIndex, final ByteBuffer value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visit(int pageIndex, final float value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visit(int pageIndex, final double value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visit(int pageIndex, final int value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visit(int pageIndex, final long value) {
                if (predicate.valueMatches(value)) matchingDictionaryIndices.set(pageIndex);
              }

              @Override
              public void visitNull(int pageIndex) {
                if (predicate.nullMatches()) matchingDictionaryIndices.set(pageIndex);
              }
            };

        for (var dictionaryIndex = 0;
            dictionaryIndex < dictionaryPage.getTotalValues();
            dictionaryIndex++) {
          dictionaryPageValues.visit(dictionaryIndex, dictionaryIndex, predicateVisitor);
        }

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
