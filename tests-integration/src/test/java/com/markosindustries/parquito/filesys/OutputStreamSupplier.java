package com.markosindustries.parquito.filesys;

import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface OutputStreamSupplier {
  OutputStream get() throws IOException;
}
