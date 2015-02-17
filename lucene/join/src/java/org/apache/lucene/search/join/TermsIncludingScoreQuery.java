package org.apache.lucene.search.join;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

class TermsIncludingScoreQuery extends Query {

  final String field;
  final boolean multipleValuesPerDocument;
  final BytesRefHash terms;
  final float[] scores;
  final int[] ords;
  final Query originalQuery;
  final Query unwrittenOriginalQuery;

  TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, Query originalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    this.unwrittenOriginalQuery = originalQuery;
  }

  private TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, int[] ords, Query originalQuery, Query unwrittenOriginalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = ords;
    this.unwrittenOriginalQuery = unwrittenOriginalQuery;
  }

  @Override
  public String toString(String string) {
    return String.format(Locale.ROOT, "TermsIncludingScoreQuery{field=%s;originalQuery=%s}", field, unwrittenOriginalQuery);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    originalQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query originalQueryRewrite = originalQuery.rewrite(reader);
    if (originalQueryRewrite != originalQuery) {
      Query rewritten = new TermsIncludingScoreQuery(field, multipleValuesPerDocument, terms, scores,
          ords, originalQueryRewrite, originalQuery);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } if (!super.equals(obj)) {
      return false;
    } if (getClass() != obj.getClass()) {
      return false;
    }

    TermsIncludingScoreQuery other = (TermsIncludingScoreQuery) obj;
    if (!field.equals(other.field)) {
      return false;
    }
    if (!unwrittenOriginalQuery.equals(other.unwrittenOriginalQuery)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result += prime * field.hashCode();
    result += prime * unwrittenOriginalQuery.hashCode();
    return result;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
    final Weight originalWeight = originalQuery.createWeight(searcher, needsScores, flags);
    return new Weight(TermsIncludingScoreQuery.this) {

      private TermsEnum segmentTermsEnum;

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms != null) {
          segmentTermsEnum = terms.iterator(segmentTermsEnum);
          BytesRef spare = new BytesRef();
          PostingsEnum postingsEnum = null;
          for (int i = 0; i < TermsIncludingScoreQuery.this.terms.size(); i++) {
            if (segmentTermsEnum.seekExact(TermsIncludingScoreQuery.this.terms.get(ords[i], spare))) {
              postingsEnum = segmentTermsEnum.postings(null, postingsEnum, PostingsEnum.FLAG_NONE);
              if (postingsEnum.advance(doc) == doc) {
                final float score = TermsIncludingScoreQuery.this.scores[ords[i]];
                return new ComplexExplanation(true, score, "Score based on join value " + segmentTermsEnum.term().utf8ToString());
              }
            }
          }
        }
        return new ComplexExplanation(false, 0.0f, "Not a match");
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return originalWeight.getValueForNormalization() * TermsIncludingScoreQuery.this.getBoost() * TermsIncludingScoreQuery.this.getBoost();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        originalWeight.normalize(norm, topLevelBoost * TermsIncludingScoreQuery.this.getBoost());
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null;
        }
        
        // what is the runtime...seems ok?
        final long cost = context.reader().maxDoc() * terms.size();

        segmentTermsEnum = terms.iterator(segmentTermsEnum);
        if (multipleValuesPerDocument) {
          return new MVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
        } else {
          return new SVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
        }
      }

    };
  }
  
  class SVInOrderScorer extends Scorer {

    final DocIdSetIterator matchingDocsIterator;
    final float[] scores;
    final long cost;

    int currentDoc = -1;

    SVInOrderScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight);
      FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
      this.scores = new float[maxDoc];
      fillDocsAndScores(matchingDocs, acceptDocs, termsEnum);
      this.matchingDocsIterator = new BitSetIterator(matchingDocs, cost);
      this.cost = cost;
    }

    protected void fillDocsAndScores(FixedBitSet matchingDocs, Bits acceptDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(acceptDocs, postingsEnum, PostingsEnum.FLAG_NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postingsEnum.nextDoc()) {
            matchingDocs.set(doc);
            // In the case the same doc is also related to a another doc, a score might be overwritten. I think this
            // can only happen in a many-to-many relation
            scores[doc] = score;
          }
        }
      }
    }

    @Override
    public float score() throws IOException {
      return scores[currentDoc];
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return currentDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return currentDoc = matchingDocsIterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return currentDoc = matchingDocsIterator.advance(target);
    }

    @Override
    public IntervalIterator intervals(boolean collectIntervals)
        throws IOException {
      return null;
    }

    public long cost() {
      return cost;
    }
  }

  // This scorer deals with the fact that a document can have more than one score from multiple related documents.
  class MVInOrderScorer extends SVInOrderScorer {

    MVInOrderScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight, acceptDocs, termsEnum, maxDoc, cost);
    }

    @Override
    protected void fillDocsAndScores(FixedBitSet matchingDocs, Bits acceptDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(acceptDocs, postingsEnum, PostingsEnum.FLAG_NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postingsEnum.nextDoc()) {
            // I prefer this:
            /*if (scores[doc] < score) {
              scores[doc] = score;
              matchingDocs.set(doc);
            }*/
            // But this behaves the same as MVInnerScorer and only then the tests will pass:
            if (!matchingDocs.get(doc)) {
              scores[doc] = score;
              matchingDocs.set(doc);
            }
          }
        }
      }
    }
  }

}
