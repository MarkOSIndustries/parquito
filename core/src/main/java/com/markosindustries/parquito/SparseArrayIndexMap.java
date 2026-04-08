package com.markosindustries.parquito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparseArrayIndexMap<T> {
  private final int[] indexes;
  private final T[] indexedValues;

  SparseArrayIndexMap(final int[] indexes, final T[] indexedValues) {
    this.indexes = indexes;
    this.indexedValues = indexedValues;
  }

  public static <Value> SparseArrayIndexMap<Value> from(
      Collection<Value> values, ToIntFunction<Value> getIndex, IntFunction<Value[]> makeArray) {
    return from(values, getIndex, Function.identity(), makeArray);
  }

  public static <Input, Value> SparseArrayIndexMap<Value> from(
      Collection<Input> values,
      ToIntFunction<Input> getIndex,
      Function<Input, Value> getValue,
      IntFunction<Value[]> makeArray) {
    return from(values, SparseArrayIndexMap::allPredicate, getIndex, getValue, makeArray);
  }

  private static boolean allPredicate(Object o) {
    return true;
  }

  public static <Input, Value> SparseArrayIndexMap<Value> from(
      Collection<Input> values,
      Predicate<Input> filter,
      ToIntFunction<Input> getIndex,
      Function<Input, Value> getValue,
      IntFunction<Value[]> makeArray) {
    final var valuesByIndex =
        values.stream().filter(filter).collect(Collectors.toMap(getIndex::applyAsInt, getValue));
    final var indexes = valuesByIndex.keySet().stream().mapToInt(i -> i).sorted().toArray();
    final var indexedValues =
        makeArray.apply(indexes.length == 0 ? 0 : 1 + indexes[indexes.length - 1]);
    for (final var idWithField : valuesByIndex.entrySet()) {
      indexedValues[idWithField.getKey()] = idWithField.getValue();
    }
    return new SparseArrayIndexMap<>(indexes, indexedValues);
  }

  public T get(int index) {
    return indexedValues[index];
  }

  public T getOrDefault(int index, T defaultValue) {
    return (index < indexedValues.length && indexedValues[index] != null)
        ? indexedValues[index]
        : defaultValue;
  }

  public int[] indexes() {
    return indexes;
  }

  public Stream<T> valuesStream() {
    return Arrays.stream(indexedValues).filter(Objects::nonNull);
  }
}
