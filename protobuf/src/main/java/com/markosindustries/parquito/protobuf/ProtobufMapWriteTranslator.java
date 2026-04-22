package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MapEntry;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;

public class ProtobufMapWriteTranslator implements WriteTranslator<Object, Void> {
  private final MapGroupWriteTranslator<?, ?, ?, ?> mapGroupWriteTranslator;

  public ProtobufMapWriteTranslator(
      final Descriptors.FieldDescriptor fieldDescriptor, final ParquetSchemaNode schemaNode) {
    final var keyValueSchemaNode = schemaNode.getChildAtIndex(0);
    final var keySchemaNode = keyValueSchemaNode.getChildAtIndex(0);
    final var valueSchemaNode = keyValueSchemaNode.getChildAtIndex(1);

    final var keyField = fieldDescriptor.getMessageType().getFields().get(0);
    final var valueField = fieldDescriptor.getMessageType().getFields().get(1);

    this.mapGroupWriteTranslator =
        new MapGroupWriteTranslator<>(
            ProtobufMessageWriteTranslator.determineAppropriateTranslator(keyField, keySchemaNode),
            ProtobufMessageWriteTranslator.determineAppropriateTranslator(
                valueField, valueSchemaNode));
  }

  @Override
  public Object getField(final int childIndex, final Object mapEntries) {
    return mapEntries; // we want to just hand the list of entries to the child to deal with
  }

  @Override
  public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
    return mapGroupWriteTranslator;
  }

  @Override
  public Void translate(final Object o) {
    throw new UnsupportedOperationException(
        "A protobuf map field cannot be directly translated, as it is not a leaf node in the parquet schema");
  }

  public static class MapGroupWriteTranslator<Key, KeyWriteAs, Value, ValueWriteAs>
      implements WriteTranslator<MapEntry<Key, Value>, Void> {
    private final WriteTranslator<Key, KeyWriteAs> keyTranslator;
    private final WriteTranslator<Value, ValueWriteAs> valueTranslator;

    public MapGroupWriteTranslator(
        final WriteTranslator<Key, KeyWriteAs> keyTranslator,
        final WriteTranslator<Value, ValueWriteAs> valueTranslator) {
      this.keyTranslator = keyTranslator;
      this.valueTranslator = valueTranslator;
    }

    @Override
    public Object getField(final int childIndex, final MapEntry<Key, Value> keyValueMapEntry) {
      return switch (childIndex) {
        case 0 -> keyValueMapEntry.getKey();
        case 1 -> keyValueMapEntry.getValue();
        default ->
            throw new IndexOutOfBoundsException(
                "We shouldn't be looking for a field at child index "
                    + childIndex
                    + " on a MAP.group structure");
      };
    }

    @Override
    public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
      return switch (childIndex) {
        case 0 -> keyTranslator;
        case 1 -> valueTranslator;
        default ->
            throw new IndexOutOfBoundsException(
                "We shouldn't be looking for a write translator at child index "
                    + childIndex
                    + " on a MAP.group structure");
      };
    }

    @Override
    public Void translate(final MapEntry<Key, Value> keyValueMapEntry) {
      throw new UnsupportedOperationException(
          "A protobuf map field's key_value group node cannot be directly translated, as it is not a leaf node in the parquet schema");
    }
  }
}
