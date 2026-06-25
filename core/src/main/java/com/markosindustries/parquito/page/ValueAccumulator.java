package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ColumnType;
import com.markosindustries.parquito.bloomfilter.BloomFilter;
import com.markosindustries.parquito.encoding.EncodingWritableValues;
import it.unimi.dsi.fastutil.booleans.AbstractBooleanList;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.floats.AbstractFloatList;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Type;

public abstract class ValueAccumulator {
  protected final ColumnType columnType;
  protected int estimatedBytesRequired;
  protected int totalValues;

  private interface ThingWithOriginalIndices {
    IntArrayList originalIndices();
  }

  public ValueAccumulator(final ColumnType columnType) {
    this.columnType = columnType;
    this.totalValues = 0;
    this.estimatedBytesRequired = 0;
  }

  public int addValue(final boolean value) {
    throw new UnsupportedOperationException();
  }

  public int addValue(final ByteBuffer value) {
    throw new UnsupportedOperationException();
  }

  public int addValue(final double value) {
    throw new UnsupportedOperationException();
  }

  public int addValue(final float value) {
    throw new UnsupportedOperationException();
  }

  public int addValue(final int value) {
    throw new UnsupportedOperationException();
  }

  public int addValue(final long value) {
    throw new UnsupportedOperationException();
  }

  public int getEstimatedBytesRequired() {
    return estimatedBytesRequired;
  }

  public abstract long getNumValues();

  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.dictionary_page_header.num_values;
  }

  public abstract ReadyToWrite makeReadyToWrite();

  public void clear() {
    estimatedBytesRequired = 0;
    totalValues = 0;
  }

  public static ValueAccumulator create(final ColumnType columnType) {
    return switch (columnType.getType()) {
      case BOOLEAN -> new BooleanAccumulator(columnType);
      case INT32 -> new Int32Accumulator(columnType);
      case INT64 -> new Int64Accumulator(columnType);
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> new FloatAccumulator(columnType);
      case DOUBLE -> new DoubleAccumulator(columnType);
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new ByteBufferAccumulator(columnType);
    };
  }

  public static class BooleanAccumulator extends ValueAccumulator {
    private final Int2ObjectOpenHashMap<ValueWithOriginalIndices> valuesWithOriginalIndices;

    public BooleanAccumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new Int2ObjectOpenHashMap<>();
    }

    private record ValueWithOriginalIndices(boolean value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      public static ValueWithOriginalIndices create(boolean value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    @Override
    public int addValue(final boolean value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(
              value ? 1 : 0,
              v -> {
                estimatedBytesRequired += 4;
                return ValueWithOriginalIndices.create(v == 1);
              });

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedBooleans = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedBooleans[index++] = value;
      }
      Arrays.parallelSort(sortedBooleans, (v1, v2) -> columnType.compare(v1.value, v2.value));
      return new ReadyToWrite(sortedBooleans);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class ByteBufferAccumulator extends ValueAccumulator {
    private final HashMap<ByteBuffer, ValueWithOriginalIndices> valuesWithOriginalIndices;

    public ByteBufferAccumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new HashMap<>();
    }

    private ValueWithOriginalIndices newDistinctValue(ByteBuffer v) {
      estimatedBytesRequired += 4 + v.remaining();
      return ValueWithOriginalIndices.create(v.mark());
    }

    private int compare(ValueWithOriginalIndices v1, ValueWithOriginalIndices v2) {
      return columnType.compare(v1.value, v2.value);
    }

    private record ValueWithOriginalIndices(ByteBuffer value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      // TODO ditch these and move into "newDistinctValue"
      public static ValueWithOriginalIndices create(ByteBuffer value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    @Override
    public int addValue(final ByteBuffer value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(value, this::newDistinctValue);

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedByteBuffers = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedByteBuffers[index++] = value;
      }
      Arrays.parallelSort(sortedByteBuffers, this::compare);
      return new ReadyToWrite(sortedByteBuffers);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class DoubleAccumulator extends ValueAccumulator {
    private final Double2ObjectOpenHashMap<ValueWithOriginalIndices> valuesWithOriginalIndices;

    public DoubleAccumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new Double2ObjectOpenHashMap<>();
    }

    private record ValueWithOriginalIndices(double value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      public static ValueWithOriginalIndices create(double value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    public int addValue(final double value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(
              value,
              v -> {
                estimatedBytesRequired += 8;
                return ValueWithOriginalIndices.create(v);
              });

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedDoubles = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedDoubles[index++] = value;
      }
      Arrays.parallelSort(sortedDoubles, (v1, v2) -> columnType.compare(v1.value, v2.value));
      return new ReadyToWrite(sortedDoubles);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class FloatAccumulator extends ValueAccumulator {
    private final Float2ObjectOpenHashMap<ValueWithOriginalIndices> valuesWithOriginalIndices;

    public FloatAccumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new Float2ObjectOpenHashMap<>();
    }

    private record ValueWithOriginalIndices(float value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      public static ValueWithOriginalIndices create(float value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    public int addValue(final float value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(
              value,
              v -> {
                estimatedBytesRequired += 4;
                return ValueWithOriginalIndices.create(v);
              });

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedFloats = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedFloats[index++] = value;
      }
      Arrays.parallelSort(sortedFloats, (v1, v2) -> columnType.compare(v1.value, v2.value));
      return new ReadyToWrite(sortedFloats);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class Int32Accumulator extends ValueAccumulator {
    private final Int2ObjectOpenHashMap<ValueWithOriginalIndices> valuesWithOriginalIndices;

    public Int32Accumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new Int2ObjectOpenHashMap<>();
    }

    private record ValueWithOriginalIndices(int value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      public static ValueWithOriginalIndices create(int value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    public int addValue(final int value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(
              value,
              v -> {
                estimatedBytesRequired += 4;
                return ValueWithOriginalIndices.create(v);
              });

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedInts = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedInts[index++] = value;
      }
      Arrays.parallelSort(sortedInts, (v1, v2) -> columnType.compare(v1.value, v2.value));
      return new ReadyToWrite(sortedInts);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class Int64Accumulator extends ValueAccumulator {
    private final Long2ObjectOpenHashMap<ValueWithOriginalIndices> valuesWithOriginalIndices;

    public Int64Accumulator(final ColumnType columnType) {
      super(columnType);
      this.valuesWithOriginalIndices = new Long2ObjectOpenHashMap<>();
    }

    private record ValueWithOriginalIndices(long value, IntArrayList originalIndices)
        implements ThingWithOriginalIndices {
      public static ValueWithOriginalIndices create(long value) {
        return new ValueWithOriginalIndices(value, new IntArrayList(1));
      }
    }

    public int addValue(final long value) {
      final var bytesBefore = estimatedBytesRequired;
      final var valueWithOriginalIndices =
          valuesWithOriginalIndices.computeIfAbsent(
              value,
              v -> {
                estimatedBytesRequired += 8;
                return ValueWithOriginalIndices.create(v);
              });

      valueWithOriginalIndices.originalIndices.add(totalValues++);

      return estimatedBytesRequired - bytesBefore;
    }

    @Override
    public long getNumValues() {
      return valuesWithOriginalIndices.size();
    }

    @Override
    public ReadyToWrite makeReadyToWrite() {
      final var sortedLongs = new ValueWithOriginalIndices[valuesWithOriginalIndices.size()];
      int index = 0;
      for (final var value : valuesWithOriginalIndices.values()) {
        sortedLongs[index++] = value;
      }
      Arrays.parallelSort(sortedLongs, (v1, v2) -> columnType.compare(v1.value, v2.value));
      return new ReadyToWrite(sortedLongs);
    }

    @Override
    public void clear() {
      super.clear();
      valuesWithOriginalIndices.clear();
    }
  }

  public static class Slice implements EncodingWritableValues {
    private final Type type;
    private final ThingWithOriginalIndices[] thingsWithOriginalIndices;
    private final int[] indices;
    private final int offset;
    private final int count;

    Slice(
        final Type type,
        final ThingWithOriginalIndices[] thingsWithOriginalIndices,
        final int[] indices,
        final int offset,
        final int count) {
      this.type = type;
      this.thingsWithOriginalIndices = thingsWithOriginalIndices;
      this.indices = indices;
      this.offset = offset;
      this.count = count;
    }

    public Slice slice(int offset, int count) {
      return new Slice(type, thingsWithOriginalIndices, indices, this.offset + offset, count);
    }

    public int length() {
      return count;
    }

    public Type getType() {
      return type;
    }

    public IntList getIndices() {
      return new AbstractIntList() {
        @Override
        public int getInt(final int index) {
          return indices[offset + index];
        }

        @Override
        public int size() {
          return count;
        }
      };
    }

    public boolean getAsBoolean(final int index) {
      return ((BooleanAccumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public ByteBuffer getAsByteBuffer(final int index) {
      return ((ByteBufferAccumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value.reset();
    }

    public double getAsDouble(final int index) {
      return ((DoubleAccumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public float getAsFloat(final int index) {
      return ((FloatAccumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public int getAsInt32(final int index) {
      return ((Int32Accumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public long getAsInt64(final int index) {
      return ((Int64Accumulator.ValueWithOriginalIndices)
              thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public IntList getBooleansAsIntList() {
      return new AbstractIntList() {
        @Override
        public int getInt(final int index) {
          return getAsBoolean(index) ? 1 : 0;
        }

        @Override
        public int size() {
          return count;
        }
      };
    }

    public IntList getInt32sAsIntList() {
      return new AbstractIntList() {
        @Override
        public int getInt(final int index) {
          return getAsInt32(index);
        }

        @Override
        public int size() {
          return count;
        }
      };
    }

    public LongList getInt64sAsLongList() {
      return new AbstractLongList() {
        @Override
        public long getLong(final int index) {
          return getAsInt64(index);
        }

        @Override
        public int size() {
          return count;
        }
      };
    }
  }

  public class ReadyToWrite {
    private final ThingWithOriginalIndices[] sortedValuesWithOriginalIndices;

    ReadyToWrite(final ThingWithOriginalIndices[] sortedValuesWithOriginalIndices) {
      this.sortedValuesWithOriginalIndices = sortedValuesWithOriginalIndices;
    }

    public int getNumDistinctValues() {
      return sortedValuesWithOriginalIndices.length;
    }

    private ByteBuffer getStatsValue(final ThingWithOriginalIndices thingWithOriginalIndices) {
      return switch (columnType.getType()) {
        case BOOLEAN -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asIntBuffer()
              .put(
                  ((BooleanAccumulator.ValueWithOriginalIndices) thingWithOriginalIndices).value
                      ? 1
                      : 0);
          yield buffer;
        }
        case INT32 -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asIntBuffer()
              .put(((Int32Accumulator.ValueWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case INT64 -> {
          final var buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asLongBuffer()
              .put(((Int64Accumulator.ValueWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
        case FLOAT -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asFloatBuffer()
              .put(((FloatAccumulator.ValueWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case DOUBLE -> {
          final var buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asDoubleBuffer()
              .put(((DoubleAccumulator.ValueWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
            ((ByteBufferAccumulator.ValueWithOriginalIndices) thingWithOriginalIndices)
                .value.reset();
      };
    }

    public ByteBuffer getMinValue() {
      return getStatsValue(sortedValuesWithOriginalIndices[0]);
    }

    public ByteBuffer getMaxValue() {
      return getStatsValue(
          sortedValuesWithOriginalIndices[sortedValuesWithOriginalIndices.length - 1]);
    }

    public void fillBloomFilter(final BloomFilter bloomFilter) {
      switch (columnType.getType()) {
        case BOOLEAN ->
            throw new UnsupportedOperationException(
                "Bloom filters are not supported for Boolean types");
        case INT32 ->
            bloomFilter.insertAll(
                new IntsArrayList(
                    (Int32Accumulator.ValueWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case INT64 ->
            bloomFilter.insertAll(
                new LongsArrayList(
                    (Int64Accumulator.ValueWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
        case FLOAT ->
            bloomFilter.insertAll(
                new FloatsArrayList(
                    (FloatAccumulator.ValueWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case DOUBLE ->
            bloomFilter.insertAll(
                new DoublesArrayList(
                    (DoubleAccumulator.ValueWithOriginalIndices[])
                        sortedValuesWithOriginalIndices));
        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
            bloomFilter.insertAll(
                new ByteBuffersArrayList(
                    (ByteBufferAccumulator.ValueWithOriginalIndices[])
                        sortedValuesWithOriginalIndices));
      }
    }

    public Slice makeSlice() {
      int[] indices = new int[totalValues];
      var dictionaryIndex = 0;
      for (final var valueWithOriginalIndices : sortedValuesWithOriginalIndices) {
        for (final var originalIndex : valueWithOriginalIndices.originalIndices()) {
          indices[originalIndex] = dictionaryIndex;
        }
        dictionaryIndex++;
      }
      return new Slice(
          columnType.getType(), sortedValuesWithOriginalIndices, indices, 0, totalValues);
    }

    public Slice sliceDistinctValues() {
      int[] indices = new int[sortedValuesWithOriginalIndices.length];
      for (var i = 0; i < indices.length; i++) {
        indices[i] = i;
      }
      return new Slice(
          columnType.getType(),
          sortedValuesWithOriginalIndices,
          indices,
          0,
          sortedValuesWithOriginalIndices.length);
    }
  }

  private static class BooleansArrayList extends AbstractBooleanList {
    private final BooleanAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public BooleansArrayList(
        final BooleanAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public boolean getBoolean(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }

  private static class ByteBuffersArrayList extends AbstractList<ByteBuffer> {
    private final ByteBufferAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public ByteBuffersArrayList(
        final ByteBufferAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public ByteBuffer get(final int index) {
      return valuesWithOriginalIndices[index].value.reset();
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }

  private static class DoublesArrayList extends AbstractDoubleList {
    private final DoubleAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public DoublesArrayList(
        final DoubleAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public double getDouble(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }

  private static class FloatsArrayList extends AbstractFloatList {
    private final FloatAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public FloatsArrayList(
        final FloatAccumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public float getFloat(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }

  private static class IntsArrayList extends AbstractIntList {
    private final Int32Accumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public IntsArrayList(
        final Int32Accumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public int getInt(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }

  private static class LongsArrayList extends AbstractLongList {
    private final Int64Accumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices;

    public LongsArrayList(
        final Int64Accumulator.ValueWithOriginalIndices[] valuesWithOriginalIndices) {
      this.valuesWithOriginalIndices = valuesWithOriginalIndices;
    }

    @Override
    public long getLong(final int index) {
      return valuesWithOriginalIndices[index].value;
    }

    @Override
    public int size() {
      return valuesWithOriginalIndices.length;
    }
  }
}
