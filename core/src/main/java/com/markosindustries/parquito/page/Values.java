package com.markosindustries.parquito.page;

public interface Values<ReadAs> {
  ReadAs get(int index);

  static <ReadAs> Values<ReadAs> empty() {
    return index -> {
      throw new IndexOutOfBoundsException();
    };
  }
}
