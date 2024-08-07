package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.page.DataPage;
import com.markosindustries.parquito.page.DictionaryPage;
import com.markosindustries.parquito.types.ColumnType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
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
  private static final int BLOOM_FILTER_HEADER_SIZE = 18;
  private final org.apache.parquet.format.ColumnChunk header;
  private final ColumnType<ReadAs> columnType;
  private final CompletableFuture<DictionaryPage<ReadAs>> dictionaryPage;
  private final CompletableFuture<BloomFilter> bloomFilter;
  private final long dataPageCompressedBytes;

  private ColumnChunkReader(
      final org.apache.parquet.format.ColumnChunk header,
      final ColumnType<ReadAs> columnType,
      final CompletableFuture<DictionaryPage<ReadAs>> dictionaryPage,
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

    final var dictionaryPageFuture = new CompletableFuture<DictionaryPage<ReadAs>>();
    final var bloomFilterFuture =
        (columnChunkHeader.meta_data.isSetBloom_filter_offset())
            ? byteRangeReader
                .readAsInputStream(
                    columnChunkHeader.meta_data.bloom_filter_offset, BLOOM_FILTER_HEADER_SIZE)
                .thenCompose(
                    bloomHeaderInputStream -> {
                      try (bloomHeaderInputStream) {
                        final BloomFilterHeader bloomFilterHeader =
                            Util.readBloomFilterHeader(bloomHeaderInputStream);
                        return byteRangeReader
                            .readAsBuffer(
                                columnChunkHeader.meta_data.bloom_filter_offset
                                    + BLOOM_FILTER_HEADER_SIZE,
                                bloomFilterHeader.numBytes)
                            .thenApply(bitset -> BloomFilter.from(bloomFilterHeader, bitset));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    })
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
          .thenApply(
              dictionaryBuffer -> {
                try {
                  final var dictionaryStream = new ByteBufferInputStream(dictionaryBuffer);
                  final var dictionaryPageHeader = Util.readPageHeader(dictionaryStream);
                  return new DictionaryPage<ReadAs>(
                      dictionaryPageHeader,
                      columnChunk,
                      dictionaryStream.readAsBufferView(dictionaryPageHeader.compressed_page_size));
                } catch (IOException e) {
                  throw new ParquetIOException(e);
                }
              })
          .whenComplete(
              (dictionaryPage, throwable) -> {
                if (throwable != null) {
                  dictionaryPageFuture.completeExceptionally(throwable);
                } else {
                  dictionaryPageFuture.complete(dictionaryPage);
                }
              });
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
            ? rowGroupHeader.sorting_columns.get(columnChunkIndex)
            : new SortingColumn(columnChunkIndex, false, true);
    final var columnType = ColumnType.create(columnChunkHeader, columnChunkSorting, columnSchema);
    return ColumnChunkReader.create(columnChunkHeader, columnType, byteRangeReader);
  }

  public org.apache.parquet.format.ColumnChunk getHeader() {
    return header;
  }

  public ColumnType<ReadAs> getColumnType() {
    return columnType;
  }

  public DictionaryPage<ReadAs> getDictionaryPage() {
    return dictionaryPage.join();
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter.join();
  }

  public boolean mightContainObject(Object value) {
    final var readAsClass = columnType.parquetType().getReadAsClass();
    if (readAsClass.isInstance(value)) {
      return mightContain(readAsClass.cast(value));
    }
    return false;
  }

  public boolean mightContain(ReadAs value) {
    if (hasRangeStats()
        && (columnType.compare(getStatsMin(), value) > 0
            || columnType.compare(getStatsMax(), value) < 0)) {
      return false;
    }
    if (hasBloomFilter() && !bloomFilterMightContain(value)) {
      return false;
    }
    if (hasDictionary() && !dictionaryContains(value)) {
      return false;
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

  private boolean bloomFilterMightContain(final ReadAs value) {
    final var bloomFilter = getBloomFilter();
    return bloomFilter.mightContain(value);
  }

  private boolean dictionaryContains(final ReadAs value) {
    final var dictionaryPage = getDictionaryPage();
    final var dictionaryPageValues = dictionaryPage.getValues();
    for (int i = 0; i < dictionaryPage.getNonNullValues(); i++) {
      if (dictionaryPageValues.get(i).equals(value)) {
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
        .thenApply(
            page -> {
              final var values = page.getValues();
              return IntStream.range(0, page.getNonNullValues())
                  .mapToObj(values::get)
                  .collect(Collectors.toUnmodifiableSet());
            })
        .join();
  }

  public CompletableFuture<Iterator<DataPage<ReadAs>>> readPages(ByteRangeReader byteRangeReader) {
    return byteRangeReader
        .readAsBuffer(header.file_offset, (int) dataPageCompressedBytes)
        .thenApply(
            chunkDataBuffer -> {
              return new Iterator<DataPage<ReadAs>>() {
                private final ByteBufferInputStream chunkDataBufferStream =
                    new ByteBufferInputStream(chunkDataBuffer);
                private int valuesFound = 0;

                @Override
                public boolean hasNext() {
                  return valuesFound < header.meta_data.num_values;
                }

                @Override
                public DataPage<ReadAs> next() {
                  final var pageHeader = ColumnChunkReader.readPageHeader(chunkDataBufferStream);
                  // TODO - CRC with
                  //     import java.util.zip.CRC32;
                  final var parquetPage =
                      DataPage.create(
                          ColumnChunkReader.this,
                          pageHeader,
                          chunkDataBufferStream.readAsBufferView(pageHeader.compressed_page_size));
                  valuesFound += parquetPage.getTotalValues();
                  return parquetPage;
                }
              };
            });
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
