package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.ListView;
import com.markosindustries.parquito.encoding.Encodings;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2ReferenceAVLTreeMap;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.SortedSet;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

public class DictionaryPageWriter<Value> implements ParquetPageWriter {
  private final Object2ReferenceAVLTreeMap<Value, ValueWithOriginalIndices<Value>>
      dictionaryWithOriginalIndices;
  private final ColumnChunkWriter<Value> columnChunkWriter;
  private int totalValues;

  private record ValueWithOriginalIndices<Value>(Value value, IntArrayList originalIndices) {
    public static <Value> ValueWithOriginalIndices<Value> create(Value value) {
      return new ValueWithOriginalIndices<>(value, new IntArrayList(1));
    }
  }

  public DictionaryPageWriter(final ColumnChunkWriter<Value> columnChunkWriter) {
    this.dictionaryWithOriginalIndices =
        new Object2ReferenceAVLTreeMap<>(columnChunkWriter.getColumnType().getComparator());
    this.totalValues = 0;
    this.columnChunkWriter = columnChunkWriter;
  }

  public Value addValue(final Value value) {
    final var valueWithOriginalIndices =
        dictionaryWithOriginalIndices.computeIfAbsent(
            value, v -> ValueWithOriginalIndices.create(value));

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return valueWithOriginalIndices.value;
  }

  @Override
  public PageHeader writePage(
      final Encoding encoding, final ColumnMetaData columnMetaData, final OutputStream outputStream)
      throws IOException {
    final var selectedEncoding =
        Encoding.PLAIN; // spec says it has to be PLAIN, ignore passed value
    final var pageOutputBufferStream = new ByteBufferOutputStream();

    final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedValuesOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));

    Encodings.<Value>getEncoding(selectedEncoding)
        .encode(
            ListView.of(dictionaryWithOriginalIndices.keySet()),
            uncompressedValuesOutputStream,
            columnChunkWriter);
    uncompressedValuesOutputStream.close();

    final var pageHeader =
        new PageHeader(
            PageType.DICTIONARY_PAGE,
            uncompressedValuesOutputStream.getBytesWrittenAsInt(),
            compressedValuesOutputStream.getBytesWrittenAsInt());
    pageHeader.setDictionary_page_header(
        new DictionaryPageHeader(dictionaryWithOriginalIndices.size(), selectedEncoding));

    Util.writePageHeader(pageHeader, outputStream);
    pageOutputBufferStream.writeTo(outputStream);

    return pageHeader;
  }

  @Override
  public long getNumValues() {
    return dictionaryWithOriginalIndices.size();
  }

  @Override
  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.dictionary_page_header.num_values;
  }

  public int[] indexValues(final List<Value> values) {
    int[] indices = new int[values.size()];
    var dictionaryIndex = 0;
    for (final var valueWithOriginalIndices : dictionaryWithOriginalIndices.values()) {
      for (final var originalIndex : valueWithOriginalIndices.originalIndices()) {
        indices[originalIndex] = dictionaryIndex;
      }
      dictionaryIndex++;
    }
    return indices;
  }

  public SortedSet<Value> getDistinctValues() {
    return dictionaryWithOriginalIndices.keySet();
  }
}
