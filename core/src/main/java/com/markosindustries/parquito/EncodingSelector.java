package com.markosindustries.parquito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.Type;

/** Decides which of Parquet's encodings to use for a given ColumnChunk. */
@FunctionalInterface
public interface EncodingSelector {
  /**
   * This will be called for each ColumnChunk to decide which of Parquet's encodings to use that
   * ColumnChunk.
   *
   * @param type The type of data in the column
   * @param schemaPath The path within the schema of the column
   * @param distinctValues The number of distinct (unique) non-null values
   * @param totalValues The total values in the chunk (including nulls)
   * @param totalNulls The total nulls in the chunk
   * @return The Encoding to use to write this ColumnChunk
   */
  Encoding selectEncoding(
      final Type type,
      final ParquetSchemaPath schemaPath,
      final long distinctValues,
      final long totalValues,
      final long totalNulls);

  DefaultEncodingSelector DEFAULT = new DefaultEncodingSelector(Collections.emptyMap());

  /**
   * An EncodingSelector that attempts to make decent default encoding selections for all supported
   * data types. If an override is present, that will be used instead. Overrides are useful for
   * things like forcing dictionary encoding for a given column. For anything more complex than a
   * simple override like this, it's better to implement your own EncodingSelector.
   */
  class DefaultEncodingSelector implements EncodingSelector {
    private final Map<ParquetSchemaPath, Encoding> overrides;

    public DefaultEncodingSelector(final Map<ParquetSchemaPath, Encoding> overrides) {
      this.overrides = Collections.unmodifiableMap(new HashMap<>(overrides));
    }

    @Override
    public final Encoding selectEncoding(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      final var totalNonNull = totalValues - totalNulls;
      final var maybeOverride = overrides.get(schemaPath);
      if (maybeOverride != null) {
        return maybeOverride;
      }
      if (distinctValues << 1 <= totalNonNull) {
        return Encoding.RLE_DICTIONARY;
      }
      return switch (type) {
        case BOOLEAN -> Encoding.RLE;
        case INT32 -> Encoding.DELTA_BINARY_PACKED;
        case INT64 -> Encoding.DELTA_BINARY_PACKED;
        case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
        case FLOAT -> Encoding.PLAIN;
        case DOUBLE -> Encoding.PLAIN;
        case BYTE_ARRAY -> Encoding.DELTA_LENGTH_BYTE_ARRAY;
        case FIXED_LEN_BYTE_ARRAY -> Encoding.DELTA_BYTE_ARRAY;
      };
    }
  }

  /**
   * An EncodingSelector that always chooses PLAIN unless a column override is present Mostly useful
   * for testing, but here as the simplest possible example.
   */
  class PlainEncodingSelector implements EncodingSelector {
    private final Map<ParquetSchemaPath, Encoding> overrides;

    public PlainEncodingSelector(final Map<ParquetSchemaPath, Encoding> overrides) {
      this.overrides = overrides;
    }

    @Override
    public Encoding selectEncoding(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      final var maybeOverride = overrides.get(schemaPath);
      if (maybeOverride != null) {
        return maybeOverride;
      }
      return Encoding.PLAIN;
    }
  }
}
