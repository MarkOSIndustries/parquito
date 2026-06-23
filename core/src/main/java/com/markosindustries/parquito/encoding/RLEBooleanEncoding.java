package com.markosindustries.parquito.encoding;

import static org.apache.parquet.format.Encoding.RLE;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import it.unimi.dsi.fastutil.ints.AbstractIntList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.parquet.format.Type;

public class RLEBooleanEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    final var type = columnChunkReader.getColumnType().getType();
    if (type != Type.BOOLEAN) {
      throw new UnsupportedOperationException("Can't use " + RLE + " with: " + type);
    }

    final var values =
        IntEncodings.INT_ENCODING_RLE.decode(expectedValues, 1, decompressedPageStream);

    return new Values() {
      @Override
      public void visit(final int pageIndex, final int valueIndex, final Visitor visitor) {
        visitor.visit(pageIndex, values[valueIndex] != 0);
      }

      @Override
      public int count() {
        return expectedValues;
      }
    };
  }

  @Override
  public void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException {
    if (values.getType() != Type.BOOLEAN) {
      throw new UnsupportedOperationException("Can't use " + RLE + " with: " + values.getType());
    }

    IntEncodings.INT_ENCODING_RLE.encode(
        new AbstractIntList() {
          @Override
          public int getInt(final int index) {
            return (values.getAsBoolean(index)) ? 1 : 0;
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
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return Maths.ceilDivPow2(values.length(), 3);
  }
}
