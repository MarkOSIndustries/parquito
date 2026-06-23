package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MapEntry;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.rows.BranchAccumulator;
import java.util.List;

public class ProtobufMapWriteTranslator implements WriteTranslator<List<MapEntry<?, ?>>> {
  private final WriteTranslator<?> mapEntryTranslator;

  public ProtobufMapWriteTranslator(
      final Descriptors.FieldDescriptor fieldDescriptor, final ParquetSchemaNode schemaNode) {
    final var keyValueSchemaNode = schemaNode.getChildAtIndex(0);
    this.mapEntryTranslator =
        ProtobufMessageWriteTranslator.determineAppropriateTranslator(
            fieldDescriptor, keyValueSchemaNode);
  }

  @Override
  public void translate(
      final List<MapEntry<?, ?>> mapEntries, final BranchAccumulator accumulator) {
    if (mapEntries.isEmpty()) {
      accumulator.accumulateNull();
    } else {
      accumulator.branch(
          mapEntriesAccessor -> {
            mapEntryTranslator.translateUnsafe(
                mapEntries, mapEntriesAccessor.childBranchAccumulator(0));
          });
    }
  }
}
