package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.encoding.PlainEncoding;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
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
    private final Object2ObjectAVLTreeMap<Value, ValueWithOriginalIndices<Value>>
        sortedValuesWithOriginalIndices;

    ReadyToWrite(
        final Object2ObjectAVLTreeMap<Value, ValueWithOriginalIndices<Value>>
            sortedValuesWithOriginalIndices) {
      this.sortedValuesWithOriginalIndices = sortedValuesWithOriginalIndices;
    }

    public long getNumDistinctValues() {
      return sortedValuesWithOriginalIndices.size();
    }

    public Value getMinValue() {
      return sortedValuesWithOriginalIndices.keySet().first();
    }

    public Value getMaxValue() {
      return sortedValuesWithOriginalIndices.keySet().last();
    }

    public PageHeader writePage(
        final ColumnMetaData columnMetaData, final OutputStream outputStream) throws IOException {
      final var pageOutputBufferStream = new ByteBufferOutputStream();

      final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
      final var uncompressedValuesOutputStream =
          new ByteCountingOutputStream(
              CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));

      plainEncoding.encode(
          sortedValuesWithOriginalIndices.keySet(),
          uncompressedValuesOutputStream,
          columnChunkWriter);
      uncompressedValuesOutputStream.close();

      final var pageHeader =
          new PageHeader(
              PageType.DICTIONARY_PAGE,
              uncompressedValuesOutputStream.getBytesWrittenAsInt(),
              compressedValuesOutputStream.getBytesWrittenAsInt());
      pageHeader.setDictionary_page_header(
          new DictionaryPageHeader(sortedValuesWithOriginalIndices.size(), Encoding.PLAIN));

      Util.writePageHeader(pageHeader, outputStream);
      pageOutputBufferStream.writeTo(outputStream);

      return pageHeader;
    }

    public SortedSet<Value> getDistinctValues() {
      return sortedValuesWithOriginalIndices.keySet();
    }

    public FastDictionary<Value, ?> makeFastDictionary() {
      final var values = new ArrayList<Value>(sortedValuesWithOriginalIndices.size());
      int[] indices = new int[totalValues];
      var dictionaryIndex = 0;
      for (final var valueWithOriginalIndices : sortedValuesWithOriginalIndices.values()) {
        values.add(valueWithOriginalIndices.value());
        for (final var originalIndex : valueWithOriginalIndices.originalIndices()) {
          indices[originalIndex] = dictionaryIndex;
        }
        dictionaryIndex++;
      }
      return FastDictionary.wrap(
          values,
          FastArray.wrap(indices),
          columnChunkWriter.getColumnType().parquetType().getReadAsClass());
    }
  }

  public ReadyToWrite makeReadyToWrite() {
    final var sortedValueSet =
        new Object2ObjectAVLTreeMap<Value, ValueWithOriginalIndices<Value>>(
            columnChunkWriter.getColumnType().getComparator());
    sortedValueSet.putAll(dictionaryWithOriginalIndices);
    return new ReadyToWrite(sortedValueSet);
  }
}
