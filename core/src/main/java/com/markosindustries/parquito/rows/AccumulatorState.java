package com.markosindustries.parquito.rows;

class AccumulatorState {
  private int estimatedBytesRequired = 0;

  public void incrementEstimatedBytesRequired(final int bytesDelta) {
    estimatedBytesRequired += bytesDelta;
  }

  public int estimatedBytesRequired() {
    return estimatedBytesRequired;
  }

  public void resetEstimatedBytesRequired() {
    estimatedBytesRequired = 0;
  }
}
