package com.markosindustries.parquito.page;

import com.markosindustries.parquito.ParquetPredicate;

public interface Values<ReadAs> {
  ReadAs get(int index);

  default PredicateMatcher matcher(final ParquetPredicate<ReadAs> predicate) {
    return index -> predicate.valueMatches(get(index));
  }

  static <ReadAs> Values<ReadAs> empty() {
    return index -> {
      throw new IndexOutOfBoundsException();
    };
  }
}
