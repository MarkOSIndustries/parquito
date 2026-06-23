package com.markosindustries.parquito.page;

import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.function.Consumer;

public interface Values {
  interface Visitor {
    void visit(int pageIndex, boolean value);

    void visit(int pageIndex, ByteBuffer value);

    void visit(int pageIndex, float value);

    void visit(int pageIndex, double value);

    void visit(int pageIndex, int value);

    void visit(int pageIndex, long value);

    void visitNull(int pageIndex);
  }

  class NoOpVisitor implements Visitor {
    public static final Visitor INSTANCE = new NoOpVisitor();

    @Override
    public void visit(int pageIndex, final boolean value) {}

    @Override
    public void visit(int pageIndex, final ByteBuffer value) {}

    @Override
    public void visit(int pageIndex, final float value) {}

    @Override
    public void visit(int pageIndex, final double value) {}

    @Override
    public void visit(int pageIndex, final int value) {}

    @Override
    public void visit(int pageIndex, final long value) {}

    @Override
    public void visitNull(int pageIndex) {}
  }

  void visit(int pageIndex, int valueIndex, Visitor visitor);

  int count();

  default <T> PredicateMaterialisedMatches materialise(
      final ColumnPredicate<T, ?> predicate, final Class<T> tClass) {
    final var matchingIndices = new BitSet(count());
    final var predicateVisitor =
        new Visitor() {
          @Override
          public void visit(int pageIndex, final boolean value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visit(int pageIndex, final ByteBuffer value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visit(int pageIndex, final float value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visit(int pageIndex, final double value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visit(int pageIndex, final int value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visit(int pageIndex, final long value) {
            if (predicate.valueMatches(value)) matchingIndices.set(pageIndex);
          }

          @Override
          public void visitNull(int pageIndex) {
            if (predicate.nullMatches()) matchingIndices.set(pageIndex);
          }
        };
    for (var index = 0; index < count(); index++) {
      visit(index, index, predicateVisitor);
    }

    return matchingIndices::get;
  }

  class Empty implements Values {
    @Override
    public void visit(int pageIndex, int valueIndex, Visitor visitor) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int count() {
      return 0;
    }
  }

  static Values empty() {
    return new Empty();
  }

  abstract class CastingVisitor<T> implements Visitor {
    private final Consumer<T> consumer;
    private final Class<T> tClass;

    public CastingVisitor(Consumer<T> consumer, Class<T> tClass) {
      this.consumer = consumer;
      this.tClass = tClass;
    }

    @Override
    public void visit(int pageIndex, final boolean value) {
      consumer.accept(tClass.cast(value));
    }

    @Override
    public void visit(int pageIndex, final ByteBuffer value) {
      consumer.accept(tClass.cast(value));
    }

    @Override
    public void visit(int pageIndex, final float value) {
      consumer.accept(tClass.cast(value));
    }

    @Override
    public void visit(int pageIndex, final double value) {
      consumer.accept(tClass.cast(value));
    }

    @Override
    public void visit(int pageIndex, final int value) {
      consumer.accept(tClass.cast(value));
    }

    @Override
    public void visit(int pageIndex, final long value) {
      consumer.accept(tClass.cast(value));
    }
  }
}
