package com.markosindustries.parquito.page;

import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.nio.ByteBuffer;
import java.util.BitSet;

public interface Values {
  boolean getBoolean(int index);

  ByteBuffer getByteBuffer(int index);

  double getDouble(int index);

  float getFloat(int index);

  int getInt32(int index);

  long getInt64(int index);

  int count();

  default <T> PredicateMaterialisedMatches materialise(final ColumnPredicate<T, ?> predicate) {
    final var matchingIndices = new BitSet(count());
    for (var index = 0; index < count(); index++) {
      matchingIndices.set(index, predicate.valueMatches(this, index));
    }

    return matchingIndices::get;
  }

  interface Visitor {
    void visit(int pageIndex, Values values, int valueIndex);

    void visitNull(int pageIndex);
  }

  class Empty implements Values {
    @Override
    public boolean getBoolean(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public ByteBuffer getByteBuffer(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public double getDouble(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public float getFloat(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int getInt32(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public long getInt64(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int count() {
      return 0;
    }
  }

  abstract class Impl implements Values {
    @Override
    public boolean getBoolean(final int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getByteBuffer(final int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(final int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(final int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt32(final int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getInt64(final int index) {
      throw new UnsupportedOperationException();
    }
  }

  static Values empty() {
    return new Empty();
  }

  class NoOpVisitor implements Visitor {
    public static final NoOpVisitor INSTANCE = new NoOpVisitor();

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {}

    @Override
    public void visitNull(final int pageIndex) {}
  }
}
