package com.markosindustries.parquito.filesys;

import java.io.File;
import java.io.FileOutputStream;

public class SimpleOutputFile extends OutputStreamAdapter {
  private final File file;

  public SimpleOutputFile(File file) {
    super(() -> new FileOutputStream(file, false));
    this.file = file;
  }

  @Override
  public String getPath() {
    return file.getAbsolutePath();
  }
}
