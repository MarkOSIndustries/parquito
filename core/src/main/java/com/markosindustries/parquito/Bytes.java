package com.markosindustries.parquito;

public class Bytes {
  public static int fromKb(int kb) {
    // just to keep things sane and avoid overflow
    assert ((short) kb) == kb;

    return kb * 1024;
  }

  public static int fromMb(int mb) {
    // just to keep things sane and avoid overflow
    assert ((short) mb) == mb;

    return mb * 1024 * 1024;
  }

  public static long fromGb(int gb) {
    // just to keep things sane and avoid overflow
    assert ((short) gb) == gb;

    return gb * 1024L * 1024L * 1024L;
  }

  public static long fromTb(int tb) {
    // just to keep things sane and avoid overflow
    assert ((short) tb) == tb;

    return tb * 1024L * 1024L * 1024L * 1024L;
  }
}
