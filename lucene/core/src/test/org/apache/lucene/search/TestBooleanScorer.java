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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestBooleanScorer extends LuceneTestCase {
  private static final String FIELD = "category";
  
  public void testMethod() throws Exception {
    Directory directory = newDirectory();

    String[] values = new String[] { "1", "2", "3", "4" };

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < values.length; i++) {
      Document doc = new Document();
      doc.add(newStringField(FIELD, values[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term(FIELD, "1")), BooleanClause.Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term(FIELD, "2")), BooleanClause.Occur.SHOULD);

    BooleanQuery query = new BooleanQuery();
    query.add(booleanQuery1, BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "9")), BooleanClause.Occur.MUST_NOT);

    IndexSearcher indexSearcher = newSearcher(ir);
    ScoreDoc[] hits = indexSearcher.search(query, 1000).scoreDocs;
    assertEquals("Number of matched documents", 2, hits.length);
    ir.close();
    directory.close();
  }

  /** Throws UOE if Weight.scorer is called */
  private static class CrazyMustUseBulkScorerQuery extends Query {

    @Override
    public String toString(String field) {
      return "MustUseBulkScorerQuery";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
      return new Weight(CrazyMustUseBulkScorerQuery.this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
          throw new UnsupportedOperationException();
        }

        @Override
        public float getValueForNormalization() {
          return 1.0f;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) {
          throw new UnsupportedOperationException();
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) {
          return new BulkScorer() {
            @Override
            public int score(LeafCollector collector, int min, int max) throws IOException {
              assert min == 0;
              collector.setScorer(new FakeScorer());
              collector.collect(0);
              return DocIdSetIterator.NO_MORE_DOCS;
            }
            @Override
            public long cost() {
              return 1;
            }
          };
        }
      };
    }
  }

  /** Make sure BooleanScorer can embed another
   *  BooleanScorer. */
  public void testEmbeddedBooleanScorer() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "doctors are people who prescribe medicines of which they know little, to cure diseases of which they know less, in human beings of whom they know nothing", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(new Term("field", "little")), BooleanClause.Occur.SHOULD);
    q1.add(new TermQuery(new Term("field", "diseases")), BooleanClause.Occur.SHOULD);

    BooleanQuery q2 = new BooleanQuery();
    q2.add(q1, BooleanClause.Occur.SHOULD);
    q2.add(new CrazyMustUseBulkScorerQuery(), BooleanClause.Occur.SHOULD);

    assertEquals(1, s.search(q2, 10).totalHits);
    r.close();
    dir.close();
  }
}
