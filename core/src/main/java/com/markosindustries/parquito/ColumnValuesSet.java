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
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import org.apache.parquet.format.Statistics;

public class ColumnValuesSet<T> {
  private final LogicalTypeConverter<T> logicalTypeConverter;
  private BooleanSet booleans;
  private ObjectSet<ByteBuffer> byteBuffers;
  private FloatSet floats;
  private DoubleSet doubles;
  private IntSet ints;
  private LongSet longs;
  private final boolean containedNull;

  public ColumnValuesSet(
      LogicalTypeConverter<T> logicalTypeConverter, Collection<T> referenceValues) {
    this.logicalTypeConverter = logicalTypeConverter;

    containedNull = referenceValues.contains(null);

    switch (logicalTypeConverter.getType()) {
      case BOOLEAN -> {
        booleans = new BooleanOpenHashSet(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          booleans.add(logicalTypeConverter.toBoolean(referenceValue));
        }
      }
      case INT32 -> {
        ints = new IntOpenHashSet(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          ints.add(logicalTypeConverter.toInt32(referenceValue));
        }
      }
      case INT64 -> {
        longs = new LongOpenHashSet(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          longs.add(logicalTypeConverter.toInt64(referenceValue));
        }
      }
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> {
        floats = new FloatOpenHashSet(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          floats.add(logicalTypeConverter.toFloat(referenceValue));
        }
      }
      case DOUBLE -> {
        doubles = new DoubleOpenHashSet(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          doubles.add(logicalTypeConverter.toDouble(referenceValue));
        }
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        byteBuffers = new ObjectOpenHashSet<>(referenceValues.size());
        for (final var referenceValue : referenceValues) {
          byteBuffers.add(logicalTypeConverter.toByteBuffer(referenceValue));
        }
      }
    }
  }

  public LogicalTypeConverter<T> getLogicalTypeConverter() {
    return logicalTypeConverter;
  }

  public BooleanSet getBooleans() {
    return booleans;
  }

  public ObjectSet<ByteBuffer> getByteBuffers() {
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

  public boolean contains(final boolean value) {
    return booleans.contains(value);
  }

  public boolean contains(final ByteBuffer value) {
    return byteBuffers.contains(value);
  }

  public boolean contains(final float value) {
    return floats.contains(value);
  }

  public boolean contains(final double value) {
    return doubles.contains(value);
  }

  public boolean contains(final int value) {
    return ints.contains(value);
  }

  public boolean contains(final long value) {
    return longs.contains(value);
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
          if (columnType.compare(min, value) < 0 && columnType.compare(max, value) > 0) {
            yield true;
          }
        }
        yield false;
      }
    };
  }
}
