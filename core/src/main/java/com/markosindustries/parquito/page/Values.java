package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ParquetPredicate;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import java.util.BitSet;

public interface Values<ReadAs> {
  ReadAs get(int index);

  int count();

  default PredicateMaterialisedMatches materialise(final ParquetPredicate.Leaf<ReadAs> predicate) {
    final var matchingIndices = new BitSet(count());
    for (var index = 0; index < count(); index++) {
      matchingIndices.set(index, predicate.valueMatches(get(index)));
    }

    return matchingIndices::get;
  }

  class Empty<ReadAs> implements Values<ReadAs> {
    @Override
    public ReadAs get(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int count() {
      return 0;
    }
  }

  static <ReadAs> Values<ReadAs> empty() {
    return new Empty<>();
  }

  class Repeated<ReadAs> implements Values<ReadAs> {
    private final ReadAs value;
    private final int count;

    public Repeated(final ReadAs value, final int count) {
      this.value = value;
      this.count = count;
    }

    @Override
    public ReadAs get(final int index) {
      if (index < count) {
        return value;
      }
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int count() {
      return count;
    }
  }

  static <ReadAs> Values<ReadAs> repeated(final ReadAs repeatedValue, final int count) {
    return new Repeated<>(repeatedValue, count);
  }
}
