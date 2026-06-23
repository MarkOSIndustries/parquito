package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingOutputStream;
import com.markosindustries.parquito.CompressionCodecs;
import com.markosindustries.parquito.encoding.PlainEncoding;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

public class DictionaryPageWriter {
  private final PlainEncoding plainEncoding;

  public DictionaryPageWriter() {
    this.plainEncoding = new PlainEncoding();
  }

  public PageHeader writePage(
      final ValueAccumulator.Slice values,
      final ColumnMetaData columnMetaData,
      final OutputStream outputStream)
      throws IOException {
    final var pageOutputBufferStream = new ByteBufferOutputStream();

    final var compressedValuesOutputStream = new ByteCountingOutputStream(pageOutputBufferStream);
    final var uncompressedValuesOutputStream =
        new ByteCountingOutputStream(
            CompressionCodecs.compress(columnMetaData.codec, compressedValuesOutputStream));

    plainEncoding.encode(values, uncompressedValuesOutputStream);
    uncompressedValuesOutputStream.close();

    final var pageHeader =
        new PageHeader(
            PageType.DICTIONARY_PAGE,
            uncompressedValuesOutputStream.getBytesWrittenAsInt(),
            compressedValuesOutputStream.getBytesWrittenAsInt());
    pageHeader.setDictionary_page_header(new DictionaryPageHeader(values.length(), Encoding.PLAIN));

    Util.writePageHeader(pageHeader, outputStream);
    pageOutputBufferStream.writeTo(outputStream);

    return pageHeader;
  }
}
