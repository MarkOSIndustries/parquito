package com.markosindustries.parquito;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

class JarManifest {
  private static Attributes loadAttributesFromJarManifest() {
    try {
      Enumeration<URL> resources =
          JarManifest.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
      while (resources.hasMoreElements()) {
        try (final InputStream manifestStream = resources.nextElement().openStream()) {
          final var manifest = new Manifest(manifestStream);
          if ("core".equals(manifest.getMainAttributes().getValue("Parquito-Artifact"))) {
            return manifest.getMainAttributes();
          }
        } catch (IOException e) {
          // Just ignore it and hope it's not the manifest file we're looking for
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    throw new RuntimeException("Couldn't load manifest attributes");
  }

  public static final Attributes ATTRIBUTES = loadAttributesFromJarManifest();
  public static final String IMPLEMENTATION_TITLE = ATTRIBUTES.getValue("Implementation-Title");
  public static final String IMPLEMENTATION_VERSION = ATTRIBUTES.getValue("Implementation-Version");
  public static final String PARQUITO_GROUP = ATTRIBUTES.getValue("Parquito-Group");
  public static final String PARQUITO_ARTIFACT = ATTRIBUTES.getValue("Parquito-Artifact");
  public static final String PARQUITO_COMMIT_SHA = ATTRIBUTES.getValue("Parquito-CommitSHA");
}
