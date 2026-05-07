package com.markosindustries.parquito.encoding;

import static org.apache.parquet.format.Encoding.RLE;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import it.unimi.dsi.fastutil.ints.AbstractIntList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RLEBooleanEncoding implements ParquetEncoding<Boolean> {
  @Override
  public Values<Boolean> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<Boolean> columnChunkReader)
      throws IOException {
    final var readAsClass = columnChunkReader.getColumnType().parquetType().getReadAsClass();
    if (!readAsClass.isAssignableFrom(Boolean.class)) {
      throw new UnsupportedOperationException("Can't use " + RLE + " with: " + readAsClass);
    }

    final var values =
        IntEncodings.INT_ENCODING_RLE.decode(expectedValues, 1, decompressedPageStream);

    return new Values<Boolean>() {
      @Override
      public Boolean get(final int index) {
        return values[index] == 1;
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  @Override
  public void encode(
      final FastDictionary<Boolean, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<Boolean> columnChunkWriter)
      throws IOException {
    IntEncodings.INT_ENCODING_RLE.encode(
        new AbstractIntList() {
          @Override
          public int getInt(final int index) {
            return ((Boolean) values.getAsObject(index)) ? 1 : 0;
          }

          @Override
          public int size() {
            return values.length();
          }
        },
        1,
        uncompressedPageStream);
  }

  @Override
  public int refineBytesRequiredEstimate(
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<Boolean> columnChunkWriter) {
    return Maths.ceilDivPow2(valueCount, 3);
  }
}
