/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Random;

public class TestVersion extends LuceneTestCase {

  public void testOnOrAfter() throws Exception {
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Version v = (Version)field.get(Version.class);
        assertTrue("LATEST must be always onOrAfter("+v+")", Version.LATEST.onOrAfter(v));
      }
    }
    assertTrue(Version.LUCENE_5_0_0.onOrAfter(Version.LUCENE_4_0_0));
    assertFalse(Version.LUCENE_4_0_0.onOrAfter(Version.LUCENE_5_0_0));
    assertTrue(Version.LUCENE_4_0_0_ALPHA.onOrAfter(Version.LUCENE_4_0_0_ALPHA));
    assertTrue(Version.LUCENE_4_0_0_BETA.onOrAfter(Version.LUCENE_4_0_0_ALPHA));
    assertTrue(Version.LUCENE_4_0_0.onOrAfter(Version.LUCENE_4_0_0_ALPHA));
    assertTrue(Version.LUCENE_4_0_0.onOrAfter(Version.LUCENE_4_0_0_BETA));
  }

  public void testToString() {
    assertEquals("4.2.0", Version.LUCENE_4_2_0.toString());
    assertEquals("4.2.0", Version.LUCENE_4_2.toString());
    assertEquals("4.2.1", Version.LUCENE_4_2_1.toString());
    assertEquals("4.0.0.1", Version.LUCENE_4_0_0_ALPHA.toString());
    assertEquals("4.0.0.2", Version.LUCENE_4_0_0_BETA.toString());
  }

  public void testParseLeniently() {
    assertEquals(Version.LUCENE_4_0_0, Version.parseLeniently("4.0"));
    assertEquals(Version.LUCENE_4_0_0, Version.parseLeniently("4.0.0"));
    assertEquals(Version.LUCENE_4_0_0, Version.parseLeniently("LUCENE_40"));
    assertEquals(Version.LUCENE_4_0_0, Version.parseLeniently("LUCENE_4_0"));
    assertEquals(Version.LATEST, Version.parseLeniently("LATEST"));
    assertEquals(Version.LATEST, Version.parseLeniently("LUCENE_CURRENT"));
  }
  
  public void testParseLenientlyExceptions() {
    try {
      Version.parseLeniently("LUCENE");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }
  }

  public void testParse() {
    assertEquals(Version.LUCENE_5_0_0, Version.parse("5.0.0"));
    assertEquals(Version.LUCENE_4_1_0, Version.parse("4.1"));
    assertEquals(Version.LUCENE_4_1_0, Version.parseLeniently("4.1.0"));
    assertEquals(Version.LUCENE_4_0_0_ALPHA, Version.parseLeniently("4.0.0.1"));
    assertEquals(Version.LUCENE_4_0_0_BETA, Version.parseLeniently("4.0.0.2"));
  }

  public void testForwardsCompatibility() {
    assertTrue(Version.parse("4.7.10").onOrAfter(Version.LUCENE_4_7_2));
    assertTrue(Version.parse("4.20.0").onOrAfter(Version.LUCENE_4_8_1));
    assertTrue(Version.parse("5.10.20").onOrAfter(Version.LUCENE_5_0_0));
  }

  public void testParseExceptions() {
    try {
      Version.parse("1.0");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("LUCENE_4_0_0");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.256");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.-1");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.256");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.-1");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.1.3");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.1.-1");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.1.1");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.1.1.2");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("4.0.0.0");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }

    try {
      Version.parse("6.0.0");
      fail();
    } catch (IllegalArgumentException iae) {
      // pass
    }
  }
  
  public void testDeprecations() throws Exception {
    // all but the latest version should be deprecated
    for (Field field : Version.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
        Version v = (Version)field.get(Version.class);
        final boolean dep = field.isAnnotationPresent(Deprecated.class);
        if (v.equals(Version.LATEST) && field.getName().equals("LUCENE_CURRENT") == false) {
          assertFalse(field.getName() + " should not be deprecated", dep);
        } else {
          assertTrue(field.getName() + " should be deprecated", dep);
        }
      }
    }
  }

  public void testLatestVersionCommonBuild() {
    // common-build.xml sets 'tests.LUCENE_VERSION', if not, we skip this test!
    String commonBuildVersion = System.getProperty("tests.LUCENE_VERSION");
    assumeTrue("Null 'tests.LUCENE_VERSION' test property. You should run the tests with the official Lucene build file",
        commonBuildVersion != null);
    assertEquals("Version.LATEST does not match the one given in common-build.xml",
        Version.LATEST.toString(), commonBuildVersion);
  }

  public void testEqualsHashCode() {
    Random random = random();
    String version = "" + (4 + random.nextInt(1)) + "."  + random.nextInt(10) + "." + random.nextInt(10);
    Version v1 = Version.parseLeniently(version);
    Version v2 = Version.parseLeniently(version);
    assertEquals(v1.hashCode(), v2.hashCode());
    assertEquals(v1, v2);
    final int iters = 10 + random.nextInt(20);
    for (int i = 0; i < iters; i++) {
      String v = "" + (4 + random.nextInt(1)) + "."  + random.nextInt(10) + "." + random.nextInt(10);
      if (v.equals(version)) {
        assertEquals(Version.parseLeniently(v).hashCode(), v1.hashCode());
        assertEquals(Version.parseLeniently(v), v1);
      } else {
        assertFalse(Version.parseLeniently(v).equals(v1));
      }
    }
  }
}
