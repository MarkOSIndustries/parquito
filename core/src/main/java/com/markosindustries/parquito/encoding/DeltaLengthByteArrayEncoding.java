package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class DeltaLengthByteArrayEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    final var lengths = DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
    final var offsets = new int[lengths.length];
    int offset = 0;
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] += offset;
      offset += lengths[i];
    }
    final var bytes = ByteBuffer.wrap(decompressedPageStream.readAllBytes());

    return index -> columnChunkReader.readValue(bytes.slice(offsets[index], lengths[index]));
  }

  @Override
  public void encode(
      final List<ReadAs> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    final var lengths = new int[values.size()];
    var totalBytesForValues = 0;
    for (var i = 0; i < values.size(); i++) {
      totalBytesForValues +=
          (lengths[i] =
              columnChunkWriter
                  .getColumnType()
                  .parquetType()
                  .getRequiredBytesToWrite(values.get(i)));
    }

    DeltaBinaryPackedEncoding.encode32(lengths, uncompressedPageStream);

    final var valueBuffer = ByteBuffer.allocate(totalBytesForValues);
    for (final var value : values) {
      columnChunkWriter.getColumnType().parquetType().writeToByteBuffer(value, valueBuffer);
    }
    uncompressedPageStream.write(valueBuffer.array());
  }
}
