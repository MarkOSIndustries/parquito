package com.markosindustries.parquito.encoding;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.nio.ByteBuffer;
import org.apache.parquet.format.Type;

public interface EncodingWritableValues {
  Type getType();

  int length();

  boolean getAsBoolean(int index);

  ByteBuffer getAsByteBuffer(int index);

  double getAsDouble(int index);

  float getAsFloat(int index);

  int getAsInt32(int index);

  long getAsInt64(int index);

  IntList getBooleansAsIntList();

  IntList getInt32sAsIntList();

  LongList getInt64sAsLongList();

  IntList getIndices();
}
