package org.apache.lucene.search;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** This class only tests some basic functionality in CSQ, the main parts are mostly
 * tested by MultiTermQuery tests, explanations seems to be tested in TestExplanations! */
public class TestConstantScoreQuery extends LuceneTestCase {
  
  public void testCSQ() throws Exception {
    final Query q1 = new ConstantScoreQuery(new TermQuery(new Term("a", "b")));
    final Query q2 = new ConstantScoreQuery(new TermQuery(new Term("a", "c")));
    final Query q3 = new ConstantScoreQuery(TermRangeFilter.newStringRange("a", "b", "c", true, true));
    QueryUtils.check(q1);
    QueryUtils.check(q2);
    QueryUtils.checkEqual(q1,q1);
    QueryUtils.checkEqual(q2,q2);
    QueryUtils.checkEqual(q3,q3);
    QueryUtils.checkUnequal(q1,q2);
    QueryUtils.checkUnequal(q2,q3);
    QueryUtils.checkUnequal(q1,q3);
    QueryUtils.checkUnequal(q1, new TermQuery(new Term("a", "b")));
  }
  
  private void checkHits(IndexSearcher searcher, Query q, final float expectedScore, final String scorerClassName, final String innerScorerClassName) throws IOException {
    final int[] count = new int[1];
    searcher.search(q, new SimpleCollector() {
      private Scorer scorer;
    
      @Override
      public void setScorer(Scorer scorer) {
        this.scorer = scorer;
        assertEquals("Scorer is implemented by wrong class", scorerClassName, scorer.getClass().getName());
        if (innerScorerClassName != null && scorer instanceof ConstantScoreQuery.ConstantScoreScorer) {
          final ConstantScoreQuery.ConstantScoreScorer innerScorer = (ConstantScoreQuery.ConstantScoreScorer) scorer;
          assertEquals("inner Scorer is implemented by wrong class", innerScorerClassName, innerScorer.in.getClass().getName());
        }
      }
      
      @Override
      public void collect(int doc) throws IOException {
        assertEquals("Score differs from expected", expectedScore, this.scorer.score(), 0);
        count[0]++;
      }
      
      @Override
      public boolean needsScores() {
        return true;
      }
    });
    assertEquals("invalid number of results", 1, count[0]);
  }
  
  public void testWrapped2Times() throws Exception {
    Directory directory = null;
    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      directory = newDirectory();
      RandomIndexWriter writer = new RandomIndexWriter (random(), directory);

      Document doc = new Document();
      doc.add(newStringField("field", "term", Field.Store.NO));
      writer.addDocument(doc);

      reader = writer.getReader();
      writer.close();
      // we don't wrap with AssertingIndexSearcher in order to have the original scorer in setScorer.
      searcher = newSearcher(reader, true, false);
      
      // set a similarity that does not normalize our boost away
      searcher.setSimilarity(new DefaultSimilarity() {
        @Override
        public float queryNorm(float sumOfSquaredWeights) {
          return 1.0f;
        }
      });
      
      final Query csq1 = new ConstantScoreQuery(new TermQuery(new Term ("field", "term")));
      csq1.setBoost(2.0f);
      final Query csq2 = new ConstantScoreQuery(csq1);
      csq2.setBoost(5.0f);
      
      final BooleanQuery bq = new BooleanQuery();
      bq.add(csq1, BooleanClause.Occur.SHOULD);
      bq.add(csq2, BooleanClause.Occur.SHOULD);
      
      final Query csqbq = new ConstantScoreQuery(bq);
      csqbq.setBoost(17.0f);
      
      checkHits(searcher, csq1, csq1.getBoost(), ConstantScoreQuery.ConstantScoreScorer.class.getName(), null);
      checkHits(searcher, csq2, csq2.getBoost(), ConstantScoreQuery.ConstantScoreScorer.class.getName(), ConstantScoreQuery.ConstantScoreScorer.class.getName());
      
      // for the combined BQ, the scorer should always be BooleanScorer's BucketScorer, because our scorer supports out-of order collection!
      final String bucketScorerClass = FakeScorer.class.getName();
      checkHits(searcher, bq, csq1.getBoost() + csq2.getBoost(), bucketScorerClass, null);
      checkHits(searcher, csqbq, csqbq.getBoost(), ConstantScoreQuery.ConstantScoreScorer.class.getName(), bucketScorerClass);
    } finally {
      if (reader != null) reader.close();
      if (directory != null) directory.close();
    }
  }

  public void testConstantScoreQueryAndFilter() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "a", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("field", "b", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    Filter filterB = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "b"))));
    Query query = new ConstantScoreQuery(filterB);

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(query, filterB, 1).totalHits); // Query for field:b, Filter field:b

    Filter filterA = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "a"))));
    query = new ConstantScoreQuery(filterA);

    assertEquals(0, s.search(query, filterB, 1).totalHits); // Query field:b, Filter field:a

    r.close();
    d.close();
  }

  // LUCENE-5307
  // don't reuse the scorer of filters since they have been created with bulkScorer=false
  public void testQueryWrapperFilter() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "a", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    Filter filter = new QueryWrapperFilter(AssertingQuery.wrap(random(), new TermQuery(new Term("field", "a"))));
    IndexSearcher s = newSearcher(r);
    assert s instanceof AssertingIndexSearcher;
    // this used to fail
    s.search(new ConstantScoreQuery(filter), new TotalHitCountCollector());
    
    // check the rewrite
    Query rewritten = new ConstantScoreQuery(filter).rewrite(r);
    assertTrue(rewritten instanceof ConstantScoreQuery);
    assertTrue(((ConstantScoreQuery) rewritten).getQuery() instanceof AssertingQuery);
    
    r.close();
    d.close();
  }

}
