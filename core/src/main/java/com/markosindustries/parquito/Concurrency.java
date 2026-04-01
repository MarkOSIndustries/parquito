package com.markosindustries.parquito;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

final class Concurrency {
  public static final Executor DEFAULT_EXECUTOR =
      Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("parquito").factory());

  private Concurrency() {}
}
