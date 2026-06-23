package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.rows.BranchAccumulator;
import java.util.List;

public class ProtobufListWriteTranslator implements WriteTranslator<List<Object>> {
  private final WriteTranslator<?> branchTranslator;
  private final ProtobufMessageWriteTranslator.FieldLeafRepeatedVisitor leafVisitor;
  private final ParquetSchemaNode schemaNode;

  public ProtobufListWriteTranslator(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode schemaNode) {
    this.schemaNode = schemaNode;
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);

    this.branchTranslator =
        ProtobufMessageWriteTranslator.determineAppropriateTranslator(field, elementSchemaNode);
    this.leafVisitor =
        branchTranslator == null
            ? ProtobufMessageWriteTranslator.determineAppropriateFieldLeafRepeatedVisitor(
                field, elementSchemaNode)
            : null;
  }

  @Override
  public void translate(final List<Object> list, final BranchAccumulator accumulator) {
    if (list.isEmpty()) {
      accumulator.accumulateNull();
    } else {
      accumulator.branch(
          listGroupAccessor -> {
            final var listGroupAccumulator = listGroupAccessor.childBranchAccumulator(0);
            listGroupAccumulator.branch(
                elementAccessor -> {
                  if (branchTranslator != null) {
                    branchTranslator.translateUnsafe(
                        list, elementAccessor.childBranchAccumulator(0));
                  } else {
                    leafVisitor.visit(list, elementAccessor.childLeafAccumulator(0));
                  }
                });
          });
    }
  }
}
