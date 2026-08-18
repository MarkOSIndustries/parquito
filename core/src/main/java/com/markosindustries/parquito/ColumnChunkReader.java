package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.bloomfilter.BloomFilterRead;
import com.markosindustries.parquito.page.DataPageReader;
import com.markosindustries.parquito.page.DictionaryPageReader;
import com.markosindustries.parquito.types.ConversionStrategy;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.Util;

public class ColumnChunkReader {
  /**
   * Usually between 14/18, but a bit of over-read won't hurt because the bloom filter bitset is
   * next and it's minimum 32 bytes
   */
  private static final int BLOOM_FILTER_HEADER_SIZE = 32;

  private final ColumnChunk header;
  private final ColumnType columnType;
  private final CompletableFuture<DictionaryPageReader> dictionaryPage;
  private final CompletableFuture<BloomFilter> bloomFilter;
  private final long dataPageCompressedBytes;

  private ColumnChunkReader(
      final ColumnChunk header,
      final ColumnType columnType,
      final CompletableFuture<DictionaryPageReader> dictionaryPage,
      final CompletableFuture<BloomFilter> bloomFilter,
      final long dataPageCompressedBytes) {
    this.header = header;
    this.columnType = columnType;
    this.dictionaryPage = dictionaryPage;
    this.bloomFilter = bloomFilter;
    this.dataPageCompressedBytes = dataPageCompressedBytes;
  }

  public static ColumnChunkReader create(
      final ColumnChunk columnChunkHeader,
      final ColumnType type,
      final ByteRangeReader byteRangeReader) {
    // ColumnChunk's file_offset is deprecated in parquet-format (see PARQUET-2139) and not every
    // writer keeps it in step with the page offsets, use the data_page_offset instead.
    final var dictionarySize =
        columnChunkHeader.meta_data.isSetDictionary_page_offset()
            ? (columnChunkHeader.meta_data.data_page_offset
                - columnChunkHeader.meta_data.dictionary_page_offset)
            : 0;

    final var dictionaryPageFuture = new CompletableFuture<DictionaryPageReader>();
    final var bloomFilterFuture = readBloomFilter(byteRangeReader, columnChunkHeader.meta_data);
    final var columnChunk =
        new ColumnChunkReader(
            columnChunkHeader,
            type,
            dictionaryPageFuture,
            bloomFilterFuture,
            columnChunkHeader.meta_data.total_compressed_size - dictionarySize);
    if (columnChunkHeader.meta_data.isSetDictionary_page_offset()) {
      byteRangeReader
          .readAsBuffer(columnChunkHeader.meta_data.dictionary_page_offset, (int) dictionarySize)
          .thenApplyAsync(
              dictionaryBuffer -> {
                try {
                  final var dictionaryStream = new ByteBufferInputStream(dictionaryBuffer);
                  final var dictionaryPageHeader = Util.readPageHeader(dictionaryStream);
                  return new DictionaryPageReader(
                      dictionaryPageHeader,
                      columnChunk,
                      dictionaryStream.readAsBufferView(dictionaryPageHeader.compressed_page_size));
                } catch (IOException e) {
                  throw new ParquetIOException(e);
                }
              },
              Concurrency.DEFAULT_EXECUTOR)
          .whenCompleteAsync(
              (dictionaryPageReader, throwable) -> {
                if (throwable != null) {
                  dictionaryPageFuture.completeExceptionally(throwable);
                } else {
                  dictionaryPageFuture.complete(dictionaryPageReader);
                }
              },
              Concurrency.DEFAULT_EXECUTOR);
    } else {
      dictionaryPageFuture.completeExceptionally(
          new RuntimeException(
              "No dictionary page is present for "
                  + String.join(".", columnChunkHeader.meta_data.path_in_schema)));
    }
    return columnChunk;
  }

  public static ColumnChunkReader create(
      final RowGroup rowGroupHeader,
      final int columnChunkIndex,
      final ParquetSchemaNode columnSchema,
      final ByteRangeReader byteRangeReader) {
    final var columnChunkHeader = rowGroupHeader.columns.get(columnChunkIndex);
    final var columnChunkSorting =
        rowGroupHeader.isSetSorting_columns()
            ? rowGroupHeader.sorting_columns.stream()
                .filter(sorting -> sorting.column_idx == columnChunkIndex)
                .findAny()
                .orElseGet(() -> new SortingColumn(columnChunkIndex, false, true))
            : new SortingColumn(columnChunkIndex, false, true);
    final var columnType = ColumnType.create(columnChunkSorting, columnSchema);
    return ColumnChunkReader.create(columnChunkHeader, columnType, byteRangeReader);
  }

  public static CompletableFuture<BloomFilter> readBloomFilter(
      final ByteRangeReader byteRangeReader, final ColumnMetaData columnMetaData) {
    if (!columnMetaData.isSetBloom_filter_offset()) {
      return CompletableFuture.failedFuture(
          new RuntimeException(
              "No bloom filter is present for " + String.join(".", columnMetaData.path_in_schema)));
    }
    return byteRangeReader
        .readAsInputStream(columnMetaData.bloom_filter_offset, BLOOM_FILTER_HEADER_SIZE)
        .thenComposeAsync(
            bloomHeaderInputStream -> {
              try (bloomHeaderInputStream) {
                final var bloomFilterHeaderCountingStream =
                    new ByteCountingInputStream(bloomHeaderInputStream);
                final BloomFilterHeader bloomFilterHeader =
                    Util.readBloomFilterHeader(bloomFilterHeaderCountingStream);
                return byteRangeReader
                    .readAsBuffer(
                        columnMetaData.bloom_filter_offset
                            + bloomFilterHeaderCountingStream.getBytesRead(),
                        bloomFilterHeader.numBytes)
                    .thenApplyAsync(
                        bitset -> BloomFilter.create(bloomFilterHeader, bitset),
                        Concurrency.DEFAULT_EXECUTOR);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            Concurrency.DEFAULT_EXECUTOR);
  }

  public ColumnChunk getHeader() {
    return header;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public DictionaryPageReader getDictionaryPage() {
    return dictionaryPage.join();
  }

  public BloomFilterRead getBloomFilter() {
    return bloomFilter.join();
  }

  public boolean mightContainAnyObjects(
      Collection<Object> values, ConversionStrategy conversionStrategy) {
    final var convertedClass =
        conversionStrategy.converterFor(this.columnType.schemaNode()).getConvertedClass();
    return mightContainAny(
        values.stream().filter(convertedClass::isInstance).map(convertedClass::cast).toList(),
        conversionStrategy);
  }

  public boolean mightContainObject(final Object value, ConversionStrategy conversionStrategy) {
    final var convertedClass =
        conversionStrategy.converterFor(this.columnType.schemaNode()).getConvertedClass();
    if (convertedClass.isInstance(value)) {
      return mightContainAny(List.of(convertedClass.cast(value)), conversionStrategy);
    }
    return false;
  }

  public ColumnValuesSet<?> makeColumnValuesSet(
      final Collection<?> values, final ConversionStrategy conversionStrategy) {
    final var logicalTypeConverter = conversionStrategy.converterFor(this.columnType.schemaNode());
    return ColumnValuesSet.castFrom(logicalTypeConverter, values);
  }

  public boolean mightContainAny(
      final Collection<?> values, final ConversionStrategy conversionStrategy) {
    final var logicalTypeConverter = conversionStrategy.converterFor(this.columnType.schemaNode());
    final var valuesSet = ColumnValuesSet.castFrom(logicalTypeConverter, values);
    if (hasRangeStats() && !valuesSet.anyInRange(columnType, header.meta_data.statistics)) {
      return false;
    }
    if (hasBloomFilter() && !bloomFilterMightContainAny(valuesSet)) {
      return false;
    }
    if (hasDictionary()) {
      return dictionaryContainsAny(valuesSet);
    }
    return containsNonNulls();
  }

  public boolean hasRangeStats() {
    return header.meta_data.statistics.min_value != null
        && header.meta_data.statistics.max_value != null;
  }

  public boolean containsNonNulls() {
    return header.meta_data.statistics.null_count < header.meta_data.num_values;
  }

  public boolean hasBloomFilter() {
    return header.meta_data.isSetBloom_filter_offset();
  }

  public boolean hasDictionary() {
    return header.meta_data.isSetDictionary_page_offset();
  }

  private <T> boolean bloomFilterMightContainAny(final ColumnValuesSet<T> columnValuesSet) {
    final var bloomFilter = getBloomFilter();
    return bloomFilter.mightContainAny(columnValuesSet);
  }

  private <T> boolean dictionaryContainsAny(final ColumnValuesSet<T> columnValuesSet) {
    final var dictionaryPage = getDictionaryPage();
    final var dictionaryPageValues = dictionaryPage.getValues();
    for (int i = 0; i < dictionaryPageValues.count(); i++) {
      if (columnValuesSet.contains(dictionaryPageValues, i)) {
        return true;
      }
    }
    return false;
  }

  public Set<?> getValuesInDictionary(final ConversionStrategy conversionStrategy) {
    return getValuesInDictionary(conversionStrategy.converterFor(this.columnType.schemaNode()));
  }

  private <T> Set<T> getValuesInDictionary(final LogicalTypeConverter<T> logicalTypeConverter) {
    if (!hasDictionary()) {
      return Collections.emptySet();
    }
    return dictionaryPage
        .thenApplyAsync(
            page -> {
              final var hashSet = new HashSet<T>();
              final var dictionaryPageValues = page.getValues();
              for (int i = 0; i < dictionaryPageValues.count(); i++) {
                hashSet.add(logicalTypeConverter.from(dictionaryPageValues, i));
              }
              return hashSet;
            },
            Concurrency.DEFAULT_EXECUTOR)
        .join();
  }

  public CompletableFuture<Iterator<DataPageReader>> readPages(ByteRangeReader byteRangeReader) {
    return byteRangeReader
        // file_offset is deprecated in parquet-format (see PARQUET-2139), use the dictionary's
        // data_page_offset instead.
        .readAsBuffer(header.meta_data.data_page_offset, (int) dataPageCompressedBytes)
        .thenApplyAsync(
            chunkDataBuffer -> {
              return new Iterator<DataPageReader>() {
                private final ByteBufferInputStream chunkDataBufferStream =
                    new ByteBufferInputStream(chunkDataBuffer);
                private int valuesFound = 0;

                @Override
                public boolean hasNext() {
                  return valuesFound < header.meta_data.num_values;
                }

                @Override
                public DataPageReader next() {
                  final var pageHeader = ColumnChunkReader.readPageHeader(chunkDataBufferStream);
                  // TODO - CRC with
                  //     import java.util.zip.CRC32;
                  final var parquetPage =
                      DataPageReader.create(
                          ColumnChunkReader.this,
                          pageHeader,
                          chunkDataBufferStream.readAsBufferView(pageHeader.compressed_page_size));
                  valuesFound += parquetPage.getTotalValues();
                  return parquetPage;
                }
              };
            },
            Concurrency.DEFAULT_EXECUTOR);
  }

  static PageHeader readPageHeader(InputStream inputStream) {
    try {
      return Util.readPageHeader(inputStream);
    } catch (IOException e) {
      throw new ParquetIOException(e);
    }
  }

  public Object getStatsMin(final ConversionStrategy conversionStrategy) {
    return readStatsValue(header.meta_data.statistics.min_value, conversionStrategy);
  }

  public Object getStatsMax(final ConversionStrategy conversionStrategy) {
    return readStatsValue(header.meta_data.statistics.max_value, conversionStrategy);
  }

  public Object readStatsValue(
      final ByteBuffer encoded, final ConversionStrategy conversionStrategy) {
    final var converter = conversionStrategy.converterFor(this.columnType.schemaNode());
    return switch (converter.getType()) {
      case BOOLEAN ->
          converter.fromBoolean(encoded.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get() != 0);
      case INT32 -> converter.fromInt32(encoded.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get());
      case INT64 ->
          converter.fromInt64(encoded.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get());
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT ->
          converter.fromFloat(encoded.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get());
      case DOUBLE ->
          converter.fromDouble(encoded.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get());
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> converter.fromByteBuffer(encoded.slice());
    };
  }

  @Override
  public String toString() {
    return "ColumnChunk{" + String.join(".", header.meta_data.path_in_schema) + "}";
  }
}
