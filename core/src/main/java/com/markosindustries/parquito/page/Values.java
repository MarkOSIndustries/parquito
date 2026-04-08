package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ParquetPredicate;

public interface Values<ReadAs> {
  ReadAs get(int index);

  default PredicateMatcher matcher(final ParquetPredicate<ReadAs> predicate) {
    if (predicate instanceof ParquetPredicate.All<ReadAs>) {
      return index -> true;
    }

    return index -> predicate.valueMatches(get(index));
  }

  static <ReadAs> Values<ReadAs> empty() {
    return index -> {
      throw new IndexOutOfBoundsException();
    };
  }
}
