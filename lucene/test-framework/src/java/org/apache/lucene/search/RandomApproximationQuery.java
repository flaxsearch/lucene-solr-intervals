package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Random;

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

/**
 * A {@link Query} that adds random approximations to its scorers.
 */
public class RandomApproximationQuery extends Query {

  private final Query query;
  private final Random random;

  public RandomApproximationQuery(Query query, Random random) {
    this.query = query;
    this.random = random;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);
    if (rewritten != query) {
      return new RandomApproximationQuery(rewritten, random);
    }
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RandomApproximationQuery == false) {
      return false;
    }
    final RandomApproximationQuery that = (RandomApproximationQuery) obj;
    if (this.getBoost() != that.getBoost()) {
      return false;
    }
    if (this.query.equals(that.query) == false) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return 31 * query.hashCode() + Float.floatToIntBits(getBoost());
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
    final Weight weight = query.createWeight(searcher, needsScores, flags);
    return new Weight(RandomApproximationQuery.this) {

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return weight.explain(context, doc);
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return weight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        weight.normalize(norm, topLevelBoost);
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final Scorer scorer = weight.scorer(context, acceptDocs);
        if (scorer == null) {
          return null;
        }
        final RandomTwoPhaseView twoPhaseView = new RandomTwoPhaseView(random, scorer);
        return new FilterScorer(scorer) {

          @Override
          public TwoPhaseDocIdSetIterator asTwoPhaseIterator() {
            return twoPhaseView;
          }
          
        };
      }

    };
  }

  private static class RandomTwoPhaseView extends TwoPhaseDocIdSetIterator {

    private final DocIdSetIterator disi;
    private final RandomApproximation approximation;

    RandomTwoPhaseView(Random random, DocIdSetIterator disi) {
      this.disi = disi;
      this.approximation = new RandomApproximation(random, disi);
    }

    @Override
    public DocIdSetIterator approximation() {
      return approximation;
    }

    @Override
    public boolean matches() throws IOException {
      return approximation.doc == disi.docID();
    }
    
  }

  private static class RandomApproximation extends DocIdSetIterator {

    private final Random random;
    private final DocIdSetIterator disi;

    int doc = -1;
    
    public RandomApproximation(Random random, DocIdSetIterator disi) {
      this.random = random;
      this.disi = disi;
    }

    @Override
    public int docID() {
      return doc;
    }
    
    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (disi.docID() < target) {
        disi.advance(target);
      }
      if (disi.docID() == NO_MORE_DOCS) {
        return doc = NO_MORE_DOCS;
      }
      return doc = RandomInts.randomIntBetween(random, target, disi.docID());
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

}
