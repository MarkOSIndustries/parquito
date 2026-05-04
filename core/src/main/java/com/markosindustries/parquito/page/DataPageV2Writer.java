package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.WriteSpec;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.arrays.FastList32;
import com.markosindustries.parquito.encoding.Encodings;
import com.markosindustries.parquito.encoding.IntEncodings;
import com.markosindustries.parquito.encoding.Maths;
import com.markosindustries.parquito.encoding.ParquetEncoding;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

public class DataPageV2Writer<Value> implements DataPageWriter<Value> {
  private final ColumnChunkWriter<Value> columnChunkWriter;
  private final WriteSpec writeSpec;
  private final FastList32 repetitionLevels;
  private final FastList32 definitionLevels;
  private final int repetitionLevelMax;
  private final int definitionLevelMax;
  private int totalNulls;
  private int totalValues;
  private int totalRows;

  public DataPageV2Writer(ColumnChunkWriter<Value> columnChunkWriter, WriteSpec writeSpec) {
    this.columnChunkWriter = columnChunkWriter;
    this.writeSpec = writeSpec;
    this.repetitionLevelMax =
        columnChunkWriter.getColumnType().schemaNode().getRepetitionLevelMax();
    this.definitionLevelMax =
        columnChunkWriter.getColumnType().schemaNode().getDefinitionLevelMax();
    this.repetitionLevels = FastList32.createTightestFit(repetitionLevelMax);
    this.definitionLevels = FastList32.createTightestFit(definitionLevelMax);
  }

  @Override
  public void addNull(final int repetitionLevel, final int definitionLevel) {
    this.totalNulls++;
    this.totalValues++;
    if (repetitionLevel == 0) {
      this.totalRows++;
    }
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public void addValue(final int repetitionLevel, final int definitionLevel) {
    this.totalValues++;
    if (repetitionLevel == 0) {
      this.totalRows++;
    }
    repetitionLevels.add(repetitionLevel);
    definitionLevels.add(definitionLevel);
  }

  @Override
  public List<PageHeader> writePages(
      final FastDictionary<Value, ?> values,
      final int estimatedPlainBytesRequired,
      final Encoding encoding,
      final ColumnMetaData columnMetaData,
      final OutputStream outputStream)
      throws IOException {
    final var encodingImpl = Encodings.<Value>getEncoding(encoding);

    // TODO - replace with an incremental encoder so we can watch byte counts and decide when to
    // switch pages
    //  This will work ok, but we could get closer to the target page size that way
    final var refinedBytesRequiredEstimate =
        encodingImpl.refineBytesRequiredEstimate(
            values.length(), estimatedPlainBytesRequired, columnChunkWriter);
    final var pageCount =
        Math.max(1, Math.ceilDiv(refinedBytesRequiredEstimate, writeSpec.targetBytesPerDataPage()));
    final var valuesPerPage =
        Math.min(Math.ceilDiv(values.length(), pageCount), writeSpec.maxValuesPerDataPage());

    final var pageHeaders = new ArrayList<PageHeader>(pageCount);

    var valuesIndex = 0;
    for (var levelsIndex = 0; levelsIndex < repetitionLevels.length(); ) {
      //    for (var i = 0; i < pageCount; i++) {
      var nextValuesIndex = Math.min(values.length(), valuesIndex + valuesPerPage);
      var valueCount = nextValuesIndex - valuesIndex;

      final var dataPageHeaderV2 =
          new DataPageHeaderV2()
              .setNum_values(0)
              .setNum_nulls(0)
              .setNum_rows(0)
              .setEncoding(encoding);
      var nextLevelsIndex = levelsIndex;
      while (nextLevelsIndex < repetitionLevels.length()
          && (dataPageHeaderV2.num_values < valueCount
              || valueCount == 0
              || repetitionLevels.get32(nextLevelsIndex) != 0)) {
        if (definitionLevels.get32(nextLevelsIndex) == definitionLevelMax) {
          dataPageHeaderV2.num_values++;
        } else {
          dataPageHeaderV2.num_nulls++;
        }
        if (repetitionLevels.get32(nextLevelsIndex) == 0) {
          dataPageHeaderV2.num_rows++;
        }
        nextLevelsIndex++;
      }
      // We excluded nulls before for efficiency, but they also count as rows as far as the header
      // is concerned
      valueCount = dataPageHeaderV2.num_values;
      nextValuesIndex = valuesIndex + valueCount;
      dataPageHeaderV2.num_values += dataPageHeaderV2.num_nulls;

      if (dataPageHeaderV2.num_values > 0) {
        final var pageHeader =
            writePage(
                dataPageHeaderV2,
                repetitionLevels.subList(levelsIndex, nextLevelsIndex),
                definitionLevels.subList(levelsIndex, nextLevelsIndex),
                values.sliceDictionary(valuesIndex, valueCount),
                encodingImpl,
                columnMetaData,
                outputStream);
        pageHeaders.add(pageHeader);
      }

      levelsIndex = nextLevelsIndex;
      valuesIndex = nextValuesIndex;
    }

    return pageHeaders;
  }

  private PageHeader writePage(
      final DataPageHeaderV2 dataPageHeaderV2,
      final FastList32 pageRepetitionLevels,
      final FastList32 pageDefinitionLevels,
      final FastDictionary<Value, ?> pageValues,
      final ParquetEncoding<Value> encodingImpl,
      final ColumnMetaData columnMetaData,
      final OutputStream outputStream)
      throws IOException {
    // TODO - resize this better when we get to incremental encoding
    final var pageOutputBufferStream =
        new ByteBufferOutputStream(writeSpec.targetBytesPerDataPage());
    final var repetitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    IntEncodings.INT_ENCODING_DATA_PAGE_V2_LEVELS.encode(
        pageRepetitionLevels, Maths.bitWidth(repetitionLevelMax), repetitionLevelsOutputStream);
    repetitionLevelsOutputStream.close();
    final var definitionLevelsOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    IntEncodings.INT_ENCODING_DATA_PAGE_V2_LEVELS.encode(
        pageDefinitionLevels, Maths.bitWidth(definitionLevelMax), definitionLevelsOutputStream);
    definitionLevelsOutputStream.close();

    final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedValuesOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));
    encodingImpl.encode(pageValues, uncompressedValuesOutputStream, columnChunkWriter);
    uncompressedValuesOutputStream.close();

    final var levelsBytesWritten =
        repetitionLevelsOutputStream.getBytesWrittenAsInt()
            + definitionLevelsOutputStream.getBytesWrittenAsInt();

    final var pageHeader =
        new PageHeader(
            PageType.DATA_PAGE_V2,
            levelsBytesWritten + uncompressedValuesOutputStream.getBytesWrittenAsInt(),
            levelsBytesWritten + compressedValuesOutputStream.getBytesWrittenAsInt());
    pageHeader.data_page_header_v2 =
        dataPageHeaderV2
            .setIs_compressed(!columnMetaData.codec.equals(CompressionCodec.UNCOMPRESSED))
            .setRepetition_levels_byte_length(repetitionLevelsOutputStream.getBytesWrittenAsInt())
            .setDefinition_levels_byte_length(definitionLevelsOutputStream.getBytesWrittenAsInt());

    Util.writePageHeader(pageHeader, outputStream);
    pageOutputBufferStream.writeTo(outputStream);

    return pageHeader;
  }

  @Override
  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_values;
  }

  @Override
  public long getNumValues() {
    return totalValues;
  }

  @Override
  public long getNumNulls(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_nulls;
  }

  @Override
  public long getNumNulls() {
    return totalNulls;
  }

  @Override
  public long getNumRows(final PageHeader pageHeader) {
    return pageHeader.data_page_header_v2.num_rows;
  }

  @Override
  public long getNumRows() {
    return totalRows;
  }
}
