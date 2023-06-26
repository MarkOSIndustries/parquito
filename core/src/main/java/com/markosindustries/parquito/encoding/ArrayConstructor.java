package com.markosindustries.parquito.encoding;

public interface ArrayConstructor<ReadAs> {
  ReadAs[] newArray(int size);
}
