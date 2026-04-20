package com.markosindustries.parquito;

public class NoOpShredder implements Writer.Shredder<Void> {
  public static final NoOpShredder INSTANCE = new NoOpShredder();

  @Override
  public void shred(final Void unused) {}

  @Override
  public void shredNull() {}
}
