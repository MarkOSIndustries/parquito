package com.markosindustries.parquito.bloomfilter;

import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.nio.ByteBuffer;
import java.util.List;

public interface BloomFilterWrite {
  void insert(final ByteBuffer value);

  void insert(final double value);

  void insert(final float value);

  void insert(final int value);

  void insert(final long value);

  void insertAll(final List<ByteBuffer> values);

  void insertAll(final DoubleList values);

  void insertAll(final FloatList values);

  void insertAll(final IntList values);

  void insertAll(final LongList values);
}
