package com.markosindustries.parquito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class TernarySearchTreeSetTests {
  @Test
  public void contains() {
    TernarySearchTreeSet tst =
        TernarySearchTreeSet.ofUTF8Strings(List.of("cat", "cats", "bug", "up", ""));

    assertTrue(tst.contains(ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8))));
    assertTrue(tst.contains(ByteBuffer.wrap("cat".getBytes(StandardCharsets.UTF_8))));
    assertFalse(tst.contains(ByteBuffer.wrap("cbuat".getBytes(StandardCharsets.UTF_8))));
    assertTrue(tst.contains(ByteBuffer.wrap("bug".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void iterable() {
    TernarySearchTreeSet tst =
        TernarySearchTreeSet.ofUTF8Strings(List.of("cat", "cats", "bug", "up", ""));

    final var actual = new ArrayList<ByteBuffer>();
    for (final var byteBuffer : tst) {
      actual.add(byteBuffer);
    }
    assertEquals(5, actual.size());
    assertEquals("", new String(actual.get(0).array(), StandardCharsets.UTF_8));
    assertEquals("bug", new String(actual.get(1).array(), StandardCharsets.UTF_8));
    assertEquals("cat", new String(actual.get(2).array(), StandardCharsets.UTF_8));
    assertEquals("cats", new String(actual.get(3).array(), StandardCharsets.UTF_8));
    assertEquals("up", new String(actual.get(4).array(), StandardCharsets.UTF_8));
  }

  @Test
  public void growsAsElementsAreAdded() {
    TernarySearchTreeSet tst = new TernarySearchTreeSet(1);
    tst.addAll(
        Stream.of("cat", "cats", "bug", "up")
            .map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))));
  }
}
