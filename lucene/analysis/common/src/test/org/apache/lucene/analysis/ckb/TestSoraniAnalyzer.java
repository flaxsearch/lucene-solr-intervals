package org.apache.lucene.analysis.ckb;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * Test the Sorani analyzer
 */
public class TestSoraniAnalyzer extends BaseTokenStreamTestCase {
  
  /**
   * This test fails with NPE when the stopwords file is missing in classpath
   */
  public void testResourcesAvailable() {
    new SoraniAnalyzer(TEST_VERSION_CURRENT);
  }
  
  public void testStopwords() throws IOException {
    Analyzer a = new SoraniAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "ئەم پیاوە", new String[] {"پیاو"});
  }
  
  public void testCustomStopwords() throws IOException {
    Analyzer a = new SoraniAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET);
    assertAnalyzesTo(a, "ئەم پیاوە", 
        new String[] {"ئەم", "پیاو"});
  }
  
  public void testReusableTokenStream() throws IOException {
    Analyzer a = new SoraniAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "پیاوە", new String[] {"پیاو"});
    assertAnalyzesTo(a, "پیاو", new String[] {"پیاو"});
  }
  
  public void testWithStemExclusionSet() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("پیاوە");
    Analyzer a = new SoraniAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "پیاوە", new String[] { "پیاوە" });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new SoraniAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
}
