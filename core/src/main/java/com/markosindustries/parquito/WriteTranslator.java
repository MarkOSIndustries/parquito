package com.markosindustries.parquito;

public interface WriteTranslator<Value, WriteAs> {
  Object getField(final int childIndex, final Value value);

  WriteTranslator<?, ?> forChildIndex(final int childIndex);

  WriteAs translate(Value value);
}
