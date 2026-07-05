package com.markosindustries.parquito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class LazyBitSetTests {
  @Test
  public void canReadExpectedValues() {
    final var bits = new LazyBitSet(128, i -> i % 3 == 0);
    for (var i = 0; i < 128; i++) {
      assertEquals(i % 3 == 0, bits.get(i), "Failed at index " + i);
    }
  }
}
