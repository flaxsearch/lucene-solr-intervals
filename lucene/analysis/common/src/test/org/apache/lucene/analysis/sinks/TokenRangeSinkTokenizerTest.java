package org.apache.lucene.analysis.sinks;

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
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.junit.Test;

public class TokenRangeSinkTokenizerTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    TokenRangeSinkFilter sinkFilter = new TokenRangeSinkFilter(2, 4);
    String test = "The quick red fox jumped over the lazy brown dogs";
    TeeSinkTokenFilter tee = new TeeSinkTokenFilter(whitespaceMockTokenizer(test));
    TeeSinkTokenFilter.SinkTokenStream rangeToks = tee.newSinkTokenStream(sinkFilter);

    int count = 0;
    tee.reset();
    while(tee.incrementToken()) {
      count++;
    }

    int sinkCount = 0;
    rangeToks.reset();
    while (rangeToks.incrementToken()) {
      sinkCount++;
    }

    assertTrue(count + " does not equal: " + 10, count == 10);
    assertTrue("rangeToks Size: " + sinkCount + " is not: " + 2, sinkCount == 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments() throws Exception {
    new TokenRangeSinkFilter(4, 2);
  }
}