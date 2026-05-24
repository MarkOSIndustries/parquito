package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.encoding.PlainEncoding;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

public class DictionaryPageWriter<Value> {
  private final HashMap<Value, ValueWithOriginalIndices<Value>> dictionaryWithOriginalIndices;
  private final ColumnChunkWriter<Value> columnChunkWriter;
  private final PlainEncoding<Value> plainEncoding;
  private int estimatedBytesRequired;
  private int totalValues;

  private record ValueWithOriginalIndices<Value>(Value value, IntArrayList originalIndices) {
    public static <Value> ValueWithOriginalIndices<Value> create(Value value) {
      return new ValueWithOriginalIndices<>(value, new IntArrayList(1));
    }
  }

  public DictionaryPageWriter(final ColumnChunkWriter<Value> columnChunkWriter) {
    this.dictionaryWithOriginalIndices = new HashMap<>();
    this.columnChunkWriter = columnChunkWriter;
    this.plainEncoding = new PlainEncoding<Value>();
    this.totalValues = 0;
    this.estimatedBytesRequired = 0;
  }

  public int addValue(final Value value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        dictionaryWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired +=
                  columnChunkWriter
                      .getColumnType()
                      .parquetType()
                      .getRequiredBytesToWritePlain(value);
              return ValueWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int getEstimatedBytesRequired() {
    return estimatedBytesRequired;
  }

  public long getNumValues() {
    return dictionaryWithOriginalIndices.size();
  }

  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.dictionary_page_header.num_values;
  }

  public class ReadyToWrite {
    private final ValueWithOriginalIndices<Value>[] sortedValuesWithOriginalIndices;

    ReadyToWrite(final ValueWithOriginalIndices<Value>[] sortedValuesWithOriginalIndices) {
      this.sortedValuesWithOriginalIndices = sortedValuesWithOriginalIndices;
    }

    public long getNumDistinctValues() {
      return sortedValuesWithOriginalIndices.length;
    }

    public Value getMinValue() {
      return sortedValuesWithOriginalIndices[0].value;
    }

    public Value getMaxValue() {
      return sortedValuesWithOriginalIndices[sortedValuesWithOriginalIndices.length - 1].value;
    }

    public PageHeader writePage(
        final ColumnMetaData columnMetaData, final OutputStream outputStream) throws IOException {
      final var pageOutputBufferStream = new ByteBufferOutputStream();

      final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
      final var uncompressedValuesOutputStream =
          new ByteCountingOutputStream(
              CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));

      plainEncoding.encode(
          new ValuesArrayList<>(sortedValuesWithOriginalIndices),
          uncompressedValuesOutputStream,
          columnChunkWriter);
      uncompressedValuesOutputStream.close();

      final var pageHeader =
          new PageHeader(
              PageType.DICTIONARY_PAGE,
              uncompressedValuesOutputStream.getBytesWrittenAsInt(),
              compressedValuesOutputStream.getBytesWrittenAsInt());
      pageHeader.setDictionary_page_header(
          new DictionaryPageHeader(sortedValuesWithOriginalIndices.length, Encoding.PLAIN));

      Util.writePageHeader(pageHeader, outputStream);
      pageOutputBufferStream.writeTo(outputStream);

      return pageHeader;
    }

    public Collection<Value> getDistinctValues() {
      return new ValuesArrayList<>(sortedValuesWithOriginalIndices);
    }

    public FastDictionary<Value, ?> makeFastDictionary() {
      int[] indices = new int[totalValues];
      var dictionaryIndex = 0;
      for (final var valueWithOriginalIndices : sortedValuesWithOriginalIndices) {
        for (final var originalIndex : valueWithOriginalIndices.originalIndices()) {
          indices[originalIndex] = dictionaryIndex;
        }
        dictionaryIndex++;
      }
      return FastDictionary.wrap(
          new ValuesArrayList<>(sortedValuesWithOriginalIndices),
          FastArray.wrap(indices),
          columnChunkWriter.getColumnType().parquetType().getReadAsClass());
    }
  }

  public ReadyToWrite makeReadyToWrite() {
    final ValueWithOriginalIndices<Value>[] sortedValues =
        new ValueWithOriginalIndices[dictionaryWithOriginalIndices.size()];
    int index = 0;
    for (final var value : dictionaryWithOriginalIndices.values()) {
      sortedValues[index++] = value;
    }
    Arrays.parallelSort(
        sortedValues, (v1, v2) -> columnChunkWriter.getColumnType().compare(v1.value, v2.value));
    return new ReadyToWrite(sortedValues);
  }

  private static class ValuesArrayList<Value> extends AbstractList<Value> {
    private final ValueWithOriginalIndices<Value>[] valuesWithOriginalIndices;

    public ValuesArrayList(final ValueWithOriginalIndices<Value>[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public Value get(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public Iterator<Value> iterator() {
      return new Iterator<Value>() {
        int index = 0;

        @Override
        public boolean hasNext() {
          return index < valuesWithOriginalIndices.length;
        }

        @Override
        public Value next() {
          if (index >= valuesWithOriginalIndices.length) {
            throw new NoSuchElementException();
          }
          return valuesWithOriginalIndices[index++].value;
        }
      };
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }
}
