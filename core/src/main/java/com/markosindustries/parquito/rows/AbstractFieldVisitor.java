package com.markosindustries.parquito.rows;

import java.nio.ByteBuffer;

public abstract class AbstractFieldVisitor implements FieldVisitor {
  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    throw new UnsupportedOperationException("Unexpected recursion in field visitor");
  }

  @Override
  public void endBranch() {
    throw new UnsupportedOperationException("Unexpected end of branch");
  }

  @Override
  public void endRepeated() {
    throw new UnsupportedOperationException("Unexpected end of repeated");
  }

  @Override
  public void visit(final int pageIndex, final boolean value) {
    throw new UnsupportedOperationException("Unexpected boolean value");
  }

  @Override
  public void visit(final int pageIndex, final ByteBuffer value) {
    throw new UnsupportedOperationException("Unexpected ByteBuffer value");
  }

  @Override
  public void visit(final int pageIndex, final float value) {
    throw new UnsupportedOperationException("Unexpected float value");
  }

  @Override
  public void visit(final int pageIndex, final double value) {
    throw new UnsupportedOperationException("Unexpected double value");
  }

  @Override
  public void visit(final int pageIndex, final int value) {
    throw new UnsupportedOperationException("Unexpected int value");
  }

  @Override
  public void visit(final int pageIndex, final long value) {
    throw new UnsupportedOperationException("Unexpected long value");
  }

  @Override
  public void visitNull(final int pageIndex) {
    throw new UnsupportedOperationException("Unexpected null value");
  }
}
