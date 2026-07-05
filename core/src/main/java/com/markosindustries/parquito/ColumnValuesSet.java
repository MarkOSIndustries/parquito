package com.markosindustries.parquito;

import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.booleans.BooleanSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.parquet.format.Statistics;

public class ColumnValuesSet<T> {
  private final LogicalTypeConverter<T> logicalTypeConverter;
  private BooleanSet booleans;
  private TernarySearchTreeSet byteBuffers;
  private FloatSet floats;
  private DoubleSet doubles;
  private IntSet ints;
  private LongSet longs;
  private boolean containedNull;

  public static <T> ColumnValuesSet<T> castFrom(
      final LogicalTypeConverter<T> logicalTypeConverter, final Collection<?> referenceValues) {
    return new ColumnValuesSet<>(
        logicalTypeConverter,
        asTypedStream(logicalTypeConverter, referenceValues),
        referenceValues.size());
  }

  public static <T> ColumnValuesSet<T> from(
      final LogicalTypeConverter<T> logicalTypeConverter, final Collection<T> referenceValues) {
    return new ColumnValuesSet<>(
        logicalTypeConverter, referenceValues.stream(), referenceValues.size());
  }

  private static <T> Stream<T> asTypedStream(
      final LogicalTypeConverter<T> logicalTypeConverter, final Collection<?> referenceValues) {
    final var caster = logicalTypeConverter.getConvertedClass();
    return referenceValues.stream().map(caster::cast);
  }

  public ColumnValuesSet(
      final LogicalTypeConverter<T> logicalTypeConverter,
      final Stream<T> referenceValues,
      final int valueCount) {
    this.logicalTypeConverter = logicalTypeConverter;

    switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> {
        booleans = new BooleanOpenHashSet(valueCount);
        referenceValues.forEach(
            referenceValue -> {
              if (referenceValue == null) {
                containedNull = true;
              } else {
                booleans.add(logicalTypeConverter.toBoolean(referenceValue));
              }
            });
      }
      case INT32 -> {
        ints = new IntOpenHashSet(valueCount);
        referenceValues.forEach(
            referenceValue -> {
              if (referenceValue == null) {
                containedNull = true;
              } else {
                ints.add(logicalTypeConverter.toInt32(referenceValue));
              }
            });
      }
      case INT64 -> {
        longs = new LongOpenHashSet(valueCount);
        referenceValues.forEach(
            referenceValue -> {
              if (referenceValue == null) {
                containedNull = true;
              } else {
                longs.add(logicalTypeConverter.toInt64(referenceValue));
              }
            });
      }
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> {
        floats = new FloatOpenHashSet(valueCount);
        referenceValues.forEach(
            referenceValue -> {
              if (referenceValue == null) {
                containedNull = true;
              } else {
                floats.add(logicalTypeConverter.toFloat(referenceValue));
              }
            });
      }
      case DOUBLE -> {
        doubles = new DoubleOpenHashSet(valueCount);
        referenceValues.forEach(
            referenceValue -> {
              if (referenceValue == null) {
                containedNull = true;
              } else {
                doubles.add(logicalTypeConverter.toDouble(referenceValue));
              }
            });
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        byteBuffers =
            TernarySearchTreeSet.of(
                referenceValues, valueCount, logicalTypeConverter::toByteBuffer);
      }
    }
  }

  public LogicalTypeConverter<T> getLogicalTypeConverter() {
    return logicalTypeConverter;
  }

  public BooleanSet getBooleans() {
    return booleans;
  }

  public Iterable<ByteBuffer> getByteBuffers() {
    return byteBuffers;
  }

  public FloatSet getFloats() {
    return floats;
  }

  public DoubleSet getDoubles() {
    return doubles;
  }

  public IntSet getInts() {
    return ints;
  }

  public LongSet getLongs() {
    return longs;
  }

  public boolean isEmpty() {
    return switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> booleans.isEmpty();
      case INT32 -> ints.isEmpty();
      case INT64 -> longs.isEmpty();
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT -> floats.isEmpty();
      case DOUBLE -> doubles.isEmpty();
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> byteBuffers.isEmpty();
    };
  }

  public boolean contains(final Values values, final int index) {
    return switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> booleans.contains(values.getBoolean(index));
      case INT32 -> ints.contains(values.getInt32(index));
      case INT64 -> longs.contains(values.getInt64(index));
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT -> floats.contains(values.getFloat(index));
      case DOUBLE -> doubles.contains(values.getDouble(index));
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> byteBuffers.contains(values.getByteBuffer(index));
    };
  }

  public boolean contains(final T value) {
    if (Objects.isNull(value)) {
      return containedNull;
    }
    return switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> booleans.contains(logicalTypeConverter.toBoolean(value));
      case INT32 -> ints.contains(logicalTypeConverter.toInt32(value));
      case INT64 -> longs.contains(logicalTypeConverter.toInt64(value));
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT -> floats.contains(logicalTypeConverter.toFloat(value));
      case DOUBLE -> doubles.contains(logicalTypeConverter.toDouble(value));
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
          byteBuffers.contains(logicalTypeConverter.toByteBuffer(value));
    };
  }

  public boolean containsNull() {
    return containedNull;
  }

  public boolean anyInRange(final ColumnType columnType, final Statistics statistics) {
    return switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> {
        final var min = statistics.min_value.order(ByteOrder.LITTLE_ENDIAN).getInt(0) != 0;
        final var max = statistics.max_value.order(ByteOrder.LITTLE_ENDIAN).getInt(0) != 0;
        for (final var value : booleans) {
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
      case INT32 -> {
        final var min = statistics.min_value.order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        final var max = statistics.max_value.order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        for (final var value : ints) {
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
      case INT64 -> {
        final var min = statistics.min_value.order(ByteOrder.LITTLE_ENDIAN).getLong(0);
        final var max = statistics.max_value.order(ByteOrder.LITTLE_ENDIAN).getLong(0);
        for (final var value : longs) {
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> {
        final var min = statistics.min_value.order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
        final var max = statistics.max_value.order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
        for (final var value : floats) {
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
      case DOUBLE -> {
        final var min = statistics.min_value.order(ByteOrder.LITTLE_ENDIAN).getDouble(0);
        final var max = statistics.max_value.order(ByteOrder.LITTLE_ENDIAN).getDouble(0);
        for (final var value : doubles) {
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        final var min = statistics.min_value;
        final var max = statistics.max_value;
        for (final var value : byteBuffers) {
          if (columnType.compare(min, value) <= 0 && columnType.compare(max, value) >= 0) {
            yield true;
          }
        }
        yield false;
      }
    };
  }
}
