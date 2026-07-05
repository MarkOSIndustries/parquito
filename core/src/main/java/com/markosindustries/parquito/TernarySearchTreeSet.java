package com.markosindustries.parquito;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

public class TernarySearchTreeSet implements Iterable<ByteBuffer> {
  private boolean matchesEmptyBuffer;
  private byte[] bytes;
  private BitSet matches;
  private int[] leftIndex;
  private int[] midIndex;
  private int[] rightIndex;
  private int nodeCount = 0;
  private int nodeCapacity = 0;
  private int longestEntry = 0;

  public TernarySearchTreeSet(final int initialNodeCapacity) {
    if (initialNodeCapacity < 0) {
      throw new IllegalArgumentException("Must have non-negative initial capacity");
    }
    this.bytes = new byte[initialNodeCapacity];
    this.matches = new BitSet(initialNodeCapacity);
    this.leftIndex = new int[initialNodeCapacity];
    this.midIndex = new int[initialNodeCapacity];
    this.rightIndex = new int[initialNodeCapacity];
    this.nodeCapacity = initialNodeCapacity;
  }

  public TernarySearchTreeSet() {
    this(512);
  }

  private void forceCapacity(final int newNodeCapacity) {
    if (newNodeCapacity > nodeCapacity) {
      final var nodeCapacityIncrement = Math.max(1, nodeCapacity >> 1);
      while (nodeCapacity < newNodeCapacity) {
        nodeCapacity += nodeCapacityIncrement;
      }
      this.bytes = ByteArrays.forceCapacity(this.bytes, nodeCapacity, nodeCount);
      this.leftIndex = IntArrays.forceCapacity(this.leftIndex, nodeCapacity, nodeCount);
      this.midIndex = IntArrays.forceCapacity(this.midIndex, nodeCapacity, nodeCount);
      this.rightIndex = IntArrays.forceCapacity(this.rightIndex, nodeCapacity, nodeCount);
    }
  }

  public static <T> TernarySearchTreeSet of(
      final Stream<T> values, final int valueCount, final Function<T, ByteBuffer> asByteBuffer) {
    final var result = new TernarySearchTreeSet(valueCount);
    result.addAll(values.map(asByteBuffer));
    return result;
  }

  public static TernarySearchTreeSet ofByteBuffers(final Collection<ByteBuffer> buffers) {
    return of(buffers.stream(), buffers.size(), Function.identity());
  }

  public static TernarySearchTreeSet ofUTF8Strings(final Collection<String> strings) {
    return of(
        strings.stream(), strings.size(), s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
  }

  public void addAll(final Stream<ByteBuffer> buffers) {
    final var sorted = buffers.sorted().toList();

    if (sorted.isEmpty()) {
      return;
    }

    // Iterative bulk insert without recursion:
    // Draw circles around the previous values added, halving the radius each time
    final var remaining = new BitSet(sorted.size());
    remaining.set(0, sorted.size());

    // Use two alternating lists to avoid excessive allocations
    var centresIndex = 0;
    final var centres =
        new IntArrayList[] {new IntArrayList(sorted.size()), new IntArrayList(sorted.size())};

    int radius = Math.floorDiv(sorted.size(), 2);
    add(sorted.get(radius));
    remaining.set(radius, false);
    centres[centresIndex].add(radius);
    radius = Math.floorDiv(radius, 2);
    while (radius > 0) {
      final var nextCentresIndex = (centresIndex + 1) % centres.length;
      final var nextCentres = centres[nextCentresIndex];
      for (final var centre : centres[centresIndex]) {
        add(sorted.get(centre - radius));
        remaining.set(centre - radius, false);
        nextCentres.add(centre - radius);

        add(sorted.get(centre + radius));
        remaining.set(centre + radius, false);
        nextCentres.add(centre + radius);
      }
      centres[centresIndex].clear();
      centresIndex = nextCentresIndex;
      radius = Math.floorDiv(radius, 2);
    }

    // Add anything we haven't yet now that we're down to minimum radius
    remaining.stream()
        .forEach(
            index -> {
              add(sorted.get(index));
            });
  }

  public void addAll(final Collection<ByteBuffer> buffers) {
    addAll(buffers.stream());
  }

  public void add(final ByteBuffer buffer) {
    if (buffer == null) {
      return;
    }
    if (buffer.remaining() == 0) {
      matchesEmptyBuffer = true;
      return;
    }

    longestEntry = Math.max(longestEntry, buffer.remaining());

    int nodeIndex = 0;
    int bufferIndex = buffer.position();
    if (nodeCount == 0) {
      newNode(buffer.get(bufferIndex));
    }
    while (true) {
      byte b = buffer.get(bufferIndex);

      if (b < bytes[nodeIndex]) {
        if (leftIndex[nodeIndex] < 0) {
          // Need two separate lines so that the assign works when we grow the arrays
          final var newIndex = newNode(b);
          leftIndex[nodeIndex] = newIndex;
        }
        nodeIndex = leftIndex[nodeIndex];
      } else if (b > bytes[nodeIndex]) {
        if (rightIndex[nodeIndex] < 0) {
          // Need two separate lines so that the assign works when we grow the arrays
          final var newIndex = newNode(b);
          rightIndex[nodeIndex] = newIndex;
        }
        nodeIndex = rightIndex[nodeIndex];
      } else {
        bufferIndex++;
        if (bufferIndex == buffer.remaining()) {
          matches.set(nodeIndex);
          break;
        }
        if (midIndex[nodeIndex] < 0) {
          // Need two separate lines so that the assign works when we grow the arrays
          final var newIndex = newNode(buffer.get(bufferIndex));
          midIndex[nodeIndex] = newIndex;
        }
        nodeIndex = midIndex[nodeIndex];
      }
    }
  }

  private int newNode(final byte data) {
    final var nodeIndex = nodeCount;
    forceCapacity(nodeCount + 1);
    bytes[nodeIndex] = data;
    leftIndex[nodeIndex] = -1;
    midIndex[nodeIndex] = -1;
    rightIndex[nodeIndex] = -1;
    nodeCount++;
    return nodeIndex;
  }

  public boolean contains(final ByteBuffer buffer) {
    if (buffer == null) {
      return false;
    }
    if (buffer.remaining() == 0) {
      return matchesEmptyBuffer;
    }

    int nodeIndex = nodeCount == 0 ? -1 : 0;
    int bufferIndex = 0;
    while (nodeIndex >= 0) {
      byte b = buffer.get(bufferIndex);

      if (b < bytes[nodeIndex]) {
        nodeIndex = leftIndex[nodeIndex];
      } else if (b > bytes[nodeIndex]) {
        nodeIndex = rightIndex[nodeIndex];
      } else {
        if (bufferIndex + 1 == buffer.remaining()) {
          return matches.get(nodeIndex);
        }
        nodeIndex = midIndex[nodeIndex];
        bufferIndex++;
      }
    }
    return false;
  }

  @Override
  public Iterator<ByteBuffer> iterator() {
    final var queue =
        new ArrayList<ByteBuffer>(matches.cardinality() + 1); // the one is for the empty buffer
    if (matchesEmptyBuffer) {
      queue.add(ByteBuffer.allocate(0));
    }
    collect(nodeCount == 0 ? -1 : 0, ByteBuffer.allocate(longestEntry), queue);
    return queue.iterator();
  }

  /** In-order traversal helper to collect all keys into a queue. */
  private void collect(int nodeIndex, ByteBuffer prefix, ArrayList<ByteBuffer> queue) {
    if (nodeIndex < 0) return;

    collect(leftIndex[nodeIndex], prefix, queue);

    final var centrePrefix = prefix.slice(0, prefix.limit());
    centrePrefix.position(prefix.position());
    centrePrefix.put(bytes[nodeIndex]);
    if (matches.get(nodeIndex)) {
      final var actual = ByteBuffer.allocate(centrePrefix.position());
      centrePrefix.get(0, actual.array());
      queue.add(actual);
    }
    collect(midIndex[nodeIndex], centrePrefix, queue);

    collect(rightIndex[nodeIndex], prefix, queue);
  }

  public boolean isEmpty() {
    return nodeCount == 0;
  }
}
