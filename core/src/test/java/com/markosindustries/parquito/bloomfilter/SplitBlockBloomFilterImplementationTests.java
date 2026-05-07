package com.markosindustries.parquito.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * These tests are sourced from the awesome table in the Rust implementation docs:
 * https://arrow.apache.org/rust/parquet/bloom_filter/index.html
 */
public class SplitBlockBloomFilterImplementationTests {
  @Test
  public void blocksRequiredFor() {
    assertEquals(256, SplitBlockBloomFilterImplementation.blocksRequiredFor(10_000, 0.1));
    assertEquals(512, SplitBlockBloomFilterImplementation.blocksRequiredFor(10_000, 0.01));
    assertEquals(1024, SplitBlockBloomFilterImplementation.blocksRequiredFor(10_000, 0.001));
    assertEquals(1024, SplitBlockBloomFilterImplementation.blocksRequiredFor(10_000, 0.0001));

    assertEquals(4096, SplitBlockBloomFilterImplementation.blocksRequiredFor(100_000, 0.1));

    assertEquals(131072, SplitBlockBloomFilterImplementation.blocksRequiredFor(1_000_000, 0.00001));
    assertEquals(
        262144, SplitBlockBloomFilterImplementation.blocksRequiredFor(1_000_000, 0.000001));
  }

  @Test
  public void bytesRequiredFor() {
    assertEquals(8 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(10_000, 0.1));
    assertEquals(16 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(10_000, 0.01));
    assertEquals(32 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(10_000, 0.001));
    assertEquals(32 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(10_000, 0.0001));

    assertEquals(128 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(100_000, 0.1));

    assertEquals(
        4096 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(1_000_000, 0.00001));
    assertEquals(
        8192 * 1024, SplitBlockBloomFilterImplementation.bytesRequiredFor(1_000_000, 0.000001));
  }
}
