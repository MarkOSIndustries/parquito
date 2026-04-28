package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.bloomfilter.BloomFilterRead;
import com.markosindustries.parquito.page.DataPageReader;
import com.markosindustries.parquito.page.DictionaryPageReader;
import com.markosindustries.parquito.types.ColumnType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.Util;

public class ColumnChunkReader<ReadAs> {
  /**
   * Usually between 14/18, but a bit of over-read won't hurt because the bloom filter bitset is
   * next and it's minimum 32 bytes
   */
  private static final int BLOOM_FILTER_HEADER_SIZE = 32;

  private final org.apache.parquet.format.ColumnChunk header;
  private final ColumnType<ReadAs> columnType;
  private final CompletableFuture<DictionaryPageReader<ReadAs>> dictionaryPage;
  private final CompletableFuture<BloomFilter> bloomFilter;
  private final long dataPageCompressedBytes;

  private ColumnChunkReader(
      final org.apache.parquet.format.ColumnChunk header,
      final ColumnType<ReadAs> columnType,
      final CompletableFuture<DictionaryPageReader<ReadAs>> dictionaryPage,
      final CompletableFuture<BloomFilter> bloomFilter,
      final long dataPageCompressedBytes) {
    this.header = header;
    this.columnType = columnType;
    this.dictionaryPage = dictionaryPage;
    this.bloomFilter = bloomFilter;
    this.dataPageCompressedBytes = dataPageCompressedBytes;
  }

  public static <ReadAs> ColumnChunkReader<ReadAs> create(
      final org.apache.parquet.format.ColumnChunk columnChunkHeader,
      final ColumnType<ReadAs> type,
      final ByteRangeReader byteRangeReader) {
    // Writers attribute the first DataPage as the file_offset, not the first Page - and we want the
    // first page, which is the dictionary if it has one
    final var dictionarySize =
        columnChunkHeader.meta_data.isSetDictionary_page_offset()
            ? (columnChunkHeader.file_offset - columnChunkHeader.meta_data.dictionary_page_offset)
            : 0;

    final var dictionaryPageFuture = new CompletableFuture<DictionaryPageReader<ReadAs>>();
    final var bloomFilterFuture =
        (columnChunkHeader.meta_data.isSetBloom_filter_offset())
            ? byteRangeReader
                .readAsInputStream(
                    columnChunkHeader.meta_data.bloom_filter_offset, BLOOM_FILTER_HEADER_SIZE)
                .thenComposeAsync(
                    bloomHeaderInputStream -> {
                      try (bloomHeaderInputStream) {
                        final var bloomFilterHeaderCountingStream =
                            new ByteCountingInputStream(bloomHeaderInputStream);
                        final BloomFilterHeader bloomFilterHeader =
                            Util.readBloomFilterHeader(bloomFilterHeaderCountingStream);
                        return byteRangeReader
                            .readAsBuffer(
                                columnChunkHeader.meta_data.bloom_filter_offset
                                    + bloomFilterHeaderCountingStream.getBytesRead(),
                                bloomFilterHeader.numBytes)
                            .thenApplyAsync(
                                bitset -> BloomFilter.create(bloomFilterHeader, bitset),
                                Concurrency.DEFAULT_EXECUTOR);
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    },
                    Concurrency.DEFAULT_EXECUTOR)
            : CompletableFuture.<BloomFilter>completedFuture(null);
    final var columnChunk =
        new ColumnChunkReader<ReadAs>(
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
                  return new DictionaryPageReader<ReadAs>(
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
    }
    return columnChunk;
  }

  public static ColumnChunkReader<?> create(
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
    final var columnType =
        ColumnType.create(columnChunkHeader.meta_data, columnChunkSorting, columnSchema);
    return ColumnChunkReader.create(columnChunkHeader, columnType, byteRangeReader);
  }

  public org.apache.parquet.format.ColumnChunk getHeader() {
    return header;
  }

  public ColumnType<ReadAs> getColumnType() {
    return columnType;
  }

  public DictionaryPageReader<ReadAs> getDictionaryPage() {
    return dictionaryPage.join();
  }

  public BloomFilterRead getBloomFilter() {
    return bloomFilter.join();
  }

  public boolean mightContainAnyObjects(Collection<Object> values) {
    final var readAsClass = columnType.parquetType().getReadAsClass();
    return mightContainAny(
        values.stream().filter(readAsClass::isInstance).map(readAsClass::cast).toList());
  }

  public boolean mightContainObject(final Object value) {
    final var readAsClass = columnType.parquetType().getReadAsClass();
    if (readAsClass.isInstance(value)) {
      return mightContainAny(List.of(readAsClass.cast(value)));
    }
    return false;
  }

  public boolean mightContainAny(final Collection<ReadAs> values) {
    if (hasRangeStats()
        && (values.stream()
            .allMatch(
                value ->
                    columnType.compare(getStatsMin(), value) > 0
                        || columnType.compare(getStatsMax(), value) < 0))) {
      return false;
    }
    if (hasBloomFilter() && !bloomFilterMightContainAny(values)) {
      return false;
    }
    if (hasDictionary()) {
      return dictionaryContainsAny(values);
    }
    return containsNonNulls();
  }

  public boolean hasRangeStats() {
    return header.meta_data.statistics.min_value != null
        && header.meta_data.statistics.max_value != null;
  }

  public ReadAs readValue(ByteBuffer byteBuffer) {
    return columnType.parquetType().readFromByteBuffer(byteBuffer);
  }

  public ReadAs getStatsMin() {
    return readValue(header.meta_data.statistics.min_value);
  }

  public ReadAs getStatsMax() {
    return readValue(header.meta_data.statistics.max_value);
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

  private boolean bloomFilterMightContainAny(final Collection<ReadAs> values) {
    final var bloomFilter = getBloomFilter();
    return bloomFilter.mightContainAny(values);
  }

  private boolean dictionaryContainsAny(final Collection<ReadAs> values) {
    final var dictionaryPage = getDictionaryPage();
    final var dictionaryPageValues = dictionaryPage.getValues();
    for (int i = 0; i < dictionaryPage.getNonNullValues(); i++) {
      if (values.stream().anyMatch(dictionaryPageValues.get(i)::equals)) {
        return true;
      }
    }

    return false;
  }

  public Set<ReadAs> getValuesInDictionary() {
    if (!hasDictionary()) {
      return Collections.emptySet();
    }
    return dictionaryPage
        .thenApplyAsync(
            page -> {
              final var values = page.getValues();
              return IntStream.range(0, page.getNonNullValues())
                  .mapToObj(values::get)
                  .collect(Collectors.toUnmodifiableSet());
            },
            Concurrency.DEFAULT_EXECUTOR)
        .join();
  }

  public CompletableFuture<Iterator<DataPageReader<ReadAs>>> readPages(
      ByteRangeReader byteRangeReader) {
    return byteRangeReader
        .readAsBuffer(header.file_offset, (int) dataPageCompressedBytes)
        .thenApplyAsync(
            chunkDataBuffer -> {
              return new Iterator<DataPageReader<ReadAs>>() {
                private final ByteBufferInputStream chunkDataBufferStream =
                    new ByteBufferInputStream(chunkDataBuffer);
                private int valuesFound = 0;

                @Override
                public boolean hasNext() {
                  return valuesFound < header.meta_data.num_values;
                }

                @Override
                public DataPageReader<ReadAs> next() {
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

  @Override
  public String toString() {
    return "ColumnChunk{" + String.join(".", header.meta_data.path_in_schema) + "}";
  }
}
