package com.markosindustries.parquito.encoding;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.markosindustries.parquito.ByteBufferInputStream;
import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ByteCountingInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RLEIntEncodingTests {

  private static Stream<Arguments> encoderTestCombinations() {
    final var withLengthHeader = List.of(true, false);
    final var omitZeroWidthRuns = List.of(true, false);
    final var values =
        List.of(
            new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            new int[] {2, 1},
            new int[] {1, 1},
            new int[] {0, 0},
            new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9},
            new int[] {0, 0, 1, 0, 0, 1, 0, 0, 1});

    return Lists.cartesianProduct(withLengthHeader, omitZeroWidthRuns, values).stream()
        .map(args -> Arguments.of(args.toArray()));
  }

  @ParameterizedTest
  @MethodSource("encoderTestCombinations")
  public void roundTripWithoutHeader(
      boolean withLengthHeader, boolean omitZeroWidthRuns, int[] expectedValues) throws Exception {
    final var maxValue = Arrays.stream(expectedValues).max().getAsInt();
    final int bitWidth = Maths.bitWidth(maxValue);
    final var outputStream = new ByteBufferOutputStream();
    final var encoding = new RLEIntEncoding(withLengthHeader, omitZeroWidthRuns);
    encoding.encode(expectedValues, bitWidth, outputStream);
    final var expectedBytesRead = outputStream.size();
    outputStream.write(new byte[] {0, 0}); // add a few bytes so we can check for reader overruns
    final var inputStream =
        new ByteCountingInputStream(new ByteBufferInputStream(outputStream.asByteBuffer()));
    final var actualValues = encoding.decode(expectedValues.length, bitWidth, inputStream);

    assertEquals(expectedBytesRead, inputStream.getBytesRead());
    assertArrayEquals(expectedValues, actualValues);
  }
}
