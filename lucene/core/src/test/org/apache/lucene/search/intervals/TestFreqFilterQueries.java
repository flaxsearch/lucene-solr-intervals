package org.apache.lucene.search.intervals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.junit.Test;

import java.io.IOException;

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

public class TestFreqFilterQueries extends IntervalTestBase {

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    for (String content : DOCS) {
      Document doc = new Document();
      doc.add(newField(FIELD, content, TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }
  }

  public static final String[] DOCS = new String[] {
      "banana plum apple",
      "apple apple apple apple apple",
      "apple apple apple apple banana apple strawberry banana apple",
      "banana plum apple",
      "plum apple apple apple apple apple",
      "chicken"
  };

  @Test
  public void testMinimumFrequencyFilterQuery() throws IOException {
    IntervalFilterQuery query = new IntervalFilterQuery(makeTermQuery("apple"), new MinFrequencyFilter(5));
    checkIntervals(query, searcher, new int[][]{
        { 1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 2, 0, 0, 1, 1, 2, 2, 3, 3, 5, 5, 8, 8 },
        { 4, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 }
    });
  }

  @Test
  public void testMaximumFrequencyFilterQuery() throws IOException {
    IntervalFilterQuery query = new IntervalFilterQuery(makeTermQuery("apple"), new MaxFrequencyFilter(5));
    checkIntervals(query, searcher, new int[][]{
        { 0, 2, 2 },
        { 1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 3, 2, 2 },
        { 4, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 },
    });
  }

}
