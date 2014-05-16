package org.apache.lucene.search.intervals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
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
      "strawberry strawhat strawman"
  };

  @Test
  public void testExactFrequencyFilterQuery() throws IOException {
    IntervalFilterQuery query = new IntervalFilterQuery(makeTermQuery("apple"), new RangeFrequencyFilter(5, 5));
    checkIntervals(query, searcher, new int[][]{
        { 1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 4, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 }
    });
  }

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
    IntervalFilterQuery query = new IntervalFilterQuery(makeTermQuery("apple"), new RangeFrequencyFilter(1, 5));
    checkIntervals(query, searcher, new int[][]{
        { 0, 2, 2 },
        { 1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 3, 2, 2 },
        { 4, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 },
    });
  }

  @Test
  public void testMinFreqOverDisjunction() throws IOException {
    Query q = makeOrQuery(makeTermQuery("banana"), makeTermQuery("plum"));
    checkIntervals(new IntervalFilterQuery(q, new MinFrequencyFilter(2)), searcher, new int[][]{
        { 0, 0, 0, 1, 1 },
        { 2, 4, 4, 7, 7 },
        { 3, 0, 0, 1, 1 }
    });
  }

  @Test
  public void testMinFreqOverWildcard() throws IOException {
    PrefixQuery fq = new PrefixQuery(new Term(FIELD, "straw"));
    fq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);

    checkIntervals(fq, searcher, new int[][]{
        {2, 6, 6},
        {5, 0, 0, 1, 1, 2, 2}
    });
    checkIntervals(new IntervalFilterQuery(fq, new MinFrequencyFilter(2)), searcher, new int[][]{
        { 5, 0, 0, 1, 1, 2, 2 }
    });
  }

}
