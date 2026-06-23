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

public class ValueAccumulator {
  private final ColumnType columnType;
  private final Int2ObjectOpenHashMap<BooleanWithOriginalIndices> booleansWithOriginalIndices;
  private final HashMap<ByteBuffer, ByteBufferWithOriginalIndices> byteBuffersWithOriginalIndices;
  private final Double2ObjectOpenHashMap<DoubleWithOriginalIndices> doublesWithOriginalIndices;
  private final Float2ObjectOpenHashMap<FloatWithOriginalIndices> floatsWithOriginalIndices;
  private final Int2ObjectOpenHashMap<IntWithOriginalIndices> intsWithOriginalIndices;
  private final Long2ObjectOpenHashMap<LongWithOriginalIndices> longsWithOriginalIndices;
  private int estimatedBytesRequired;
  private int totalValues;

  private interface ThingWithOriginalIndices {
    IntArrayList originalIndices();
  }

  private record BooleanWithOriginalIndices(boolean value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static BooleanWithOriginalIndices create(boolean value) {
      return new BooleanWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  private record ByteBufferWithOriginalIndices(ByteBuffer value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static ByteBufferWithOriginalIndices create(ByteBuffer value) {
      return new ByteBufferWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  private record DoubleWithOriginalIndices(double value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static DoubleWithOriginalIndices create(double value) {
      return new DoubleWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  private record FloatWithOriginalIndices(float value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static FloatWithOriginalIndices create(float value) {
      return new FloatWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  private record IntWithOriginalIndices(int value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static IntWithOriginalIndices create(int value) {
      return new IntWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  private record LongWithOriginalIndices(long value, IntArrayList originalIndices)
      implements ThingWithOriginalIndices {
    public static LongWithOriginalIndices create(long value) {
      return new LongWithOriginalIndices(value, new IntArrayList(1));
    }
  }

  public ValueAccumulator(final ColumnType columnType) {
    this.columnType = columnType;
    this.booleansWithOriginalIndices = new Int2ObjectOpenHashMap<>();
    this.byteBuffersWithOriginalIndices = new HashMap<>();
    this.doublesWithOriginalIndices = new Double2ObjectOpenHashMap<>();
    this.floatsWithOriginalIndices = new Float2ObjectOpenHashMap<>();
    this.intsWithOriginalIndices = new Int2ObjectOpenHashMap<>();
    this.longsWithOriginalIndices = new Long2ObjectOpenHashMap<>();
    this.totalValues = 0;
    this.estimatedBytesRequired = 0;
  }

  public int addValue(final boolean value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        booleansWithOriginalIndices.computeIfAbsent(
            value ? 1 : 0,
            v -> {
              estimatedBytesRequired += 4;
              return BooleanWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int addValue(final ByteBuffer value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        byteBuffersWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired += 4 + value.remaining();
              return ByteBufferWithOriginalIndices.create(value.mark());
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int addValue(final double value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        doublesWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired += 8;
              return DoubleWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int addValue(final float value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        floatsWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired += 4;
              return FloatWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int addValue(final int value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        intsWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired += 4;
              return IntWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int addValue(final long value) {
    final var bytesBefore = estimatedBytesRequired;
    final var valueWithOriginalIndices =
        longsWithOriginalIndices.computeIfAbsent(
            value,
            v -> {
              estimatedBytesRequired += 8;
              return LongWithOriginalIndices.create(value);
            });

    valueWithOriginalIndices.originalIndices.add(totalValues++);

    return estimatedBytesRequired - bytesBefore;
  }

  public int getEstimatedBytesRequired() {
    return estimatedBytesRequired;
  }

  public long getNumValues() {
    return switch (columnType.getType()) {
      case BOOLEAN -> booleansWithOriginalIndices.size();
      case INT32 -> intsWithOriginalIndices.size();
      case INT64 -> longsWithOriginalIndices.size();
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> floatsWithOriginalIndices.size();
      case DOUBLE -> doublesWithOriginalIndices.size();
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> byteBuffersWithOriginalIndices.size();
    };
  }

  public long getNumValues(final PageHeader pageHeader) {
    return pageHeader.dictionary_page_header.num_values;
  }

  public ReadyToWrite makeReadyToWrite() {
    return new ReadyToWrite(
        switch (columnType.getType()) {
          case BOOLEAN -> {
            final var sortedBooleans =
                new BooleanWithOriginalIndices[booleansWithOriginalIndices.size()];
            int index = 0;
            for (final var value : booleansWithOriginalIndices.values()) {
              sortedBooleans[index++] = value;
            }
            Arrays.parallelSort(sortedBooleans, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedBooleans;
          }
          case INT32 -> {
            final var sortedInts = new IntWithOriginalIndices[intsWithOriginalIndices.size()];
            int index = 0;
            for (final var value : intsWithOriginalIndices.values()) {
              sortedInts[index++] = value;
            }
            Arrays.parallelSort(sortedInts, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedInts;
          }
          case INT64 -> {
            final var sortedLongs = new LongWithOriginalIndices[longsWithOriginalIndices.size()];
            int index = 0;
            for (final var value : longsWithOriginalIndices.values()) {
              sortedLongs[index++] = value;
            }
            Arrays.parallelSort(sortedLongs, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedLongs;
          }
          case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
          case FLOAT -> {
            final var sortedFloats = new FloatWithOriginalIndices[floatsWithOriginalIndices.size()];
            int index = 0;
            for (final var value : floatsWithOriginalIndices.values()) {
              sortedFloats[index++] = value;
            }
            Arrays.parallelSort(sortedFloats, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedFloats;
          }
          case DOUBLE -> {
            final var sortedDoubles =
                new DoubleWithOriginalIndices[doublesWithOriginalIndices.size()];
            int index = 0;
            for (final var value : doublesWithOriginalIndices.values()) {
              sortedDoubles[index++] = value;
            }
            Arrays.parallelSort(sortedDoubles, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedDoubles;
          }
          case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
            final var sortedByteBuffers =
                new ByteBufferWithOriginalIndices[byteBuffersWithOriginalIndices.size()];
            int index = 0;
            for (final var value : byteBuffersWithOriginalIndices.values()) {
              sortedByteBuffers[index++] = value;
            }
            Arrays.parallelSort(
                sortedByteBuffers, (v1, v2) -> columnType.compare(v1.value, v2.value));
            yield sortedByteBuffers;
          }
        });
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
      return ((BooleanWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]])
          .value;
    }

    public ByteBuffer getAsByteBuffer(final int index) {
      return ((ByteBufferWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]])
          .value.reset();
    }

    public double getAsDouble(final int index) {
      return ((DoubleWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]]).value;
    }

    public float getAsFloat(final int index) {
      return ((FloatWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]]).value;
    }

    public int getAsInt32(final int index) {
      return ((IntWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]]).value;
    }

    public long getAsInt64(final int index) {
      return ((LongWithOriginalIndices) thingsWithOriginalIndices[indices[offset + index]]).value;
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

    public int getNumValues() {
      return totalValues;
    }

    private ByteBuffer getStatsValue(final ThingWithOriginalIndices thingWithOriginalIndices) {
      return switch (columnType.getType()) {
        case BOOLEAN -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer
              .asIntBuffer()
              .put(((BooleanWithOriginalIndices) thingWithOriginalIndices).value ? 1 : 0);
          yield buffer;
        }
        case INT32 -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer.asIntBuffer().put(((IntWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case INT64 -> {
          final var buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
          buffer.asLongBuffer().put(((LongWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
        case FLOAT -> {
          final var buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          buffer.asFloatBuffer().put(((FloatWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case DOUBLE -> {
          final var buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
          buffer.asDoubleBuffer().put(((DoubleWithOriginalIndices) thingWithOriginalIndices).value);
          yield buffer;
        }
        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
            ((ByteBufferWithOriginalIndices) thingWithOriginalIndices).value.reset();
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
                new IntsArrayList((IntWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case INT64 ->
            bloomFilter.insertAll(
                new LongsArrayList((LongWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
        case FLOAT ->
            bloomFilter.insertAll(
                new FloatsArrayList((FloatWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case DOUBLE ->
            bloomFilter.insertAll(
                new DoublesArrayList(
                    (DoubleWithOriginalIndices[]) sortedValuesWithOriginalIndices));
        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
            bloomFilter.insertAll(
                new ByteBuffersArrayList(
                    (ByteBufferWithOriginalIndices[]) sortedValuesWithOriginalIndices));
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
    private final BooleanWithOriginalIndices[] valuesWithOriginalIndices;

    public BooleansArrayList(final BooleanWithOriginalIndices[] valuesWithOriginalIndices) {
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
    private final ByteBufferWithOriginalIndices[] valuesWithOriginalIndices;

    public ByteBuffersArrayList(final ByteBufferWithOriginalIndices[] valuesWithOriginalIndices) {
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
    private final DoubleWithOriginalIndices[] valuesWithOriginalIndices;

    public DoublesArrayList(final DoubleWithOriginalIndices[] valuesWithOriginalIndices) {
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
    private final FloatWithOriginalIndices[] valuesWithOriginalIndices;

    public FloatsArrayList(final FloatWithOriginalIndices[] valuesWithOriginalIndices) {
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
    private final IntWithOriginalIndices[] valuesWithOriginalIndices;

    public IntsArrayList(final IntWithOriginalIndices[] valuesWithOriginalIndices) {
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
    private final LongWithOriginalIndices[] valuesWithOriginalIndices;

    public LongsArrayList(final LongWithOriginalIndices[] valuesWithOriginalIndices) {
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
