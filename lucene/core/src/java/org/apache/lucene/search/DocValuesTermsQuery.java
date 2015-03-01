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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link Query} that only accepts documents whose
 * term value in the specified field is contained in the
 * provided set of allowed terms.
 *
 * <p>
 * This is the same functionality as TermsQuery (from
 * queries/), but because of drastically different
 * implementations, they also have different performance
 * characteristics, as described below.
 *
 * <p>
 * With each search, this query translates the specified
 * set of Terms into a private {@link LongBitSet} keyed by
 * term number per unique {@link IndexReader} (normally one
 * reader per segment).  Then, during matching, the term
 * number for each docID is retrieved from the cache and
 * then checked for inclusion using the {@link LongBitSet}.
 * Since all testing is done using RAM resident data
 * structures, performance should be very fast, most likely
 * fast enough to not require further caching of the
 * DocIdSet for each possible combination of terms.
 * However, because docIDs are simply scanned linearly, an
 * index with a great many small documents may find this
 * linear scan too costly.
 *
 * <p>
 * In contrast, TermsQuery builds up an {@link FixedBitSet},
 * keyed by docID, every time it's created, by enumerating
 * through all matching docs using {@link org.apache.lucene.index.PostingsEnum} to seek
 * and scan through each term's docID list.  While there is
 * no linear scan of all docIDs, besides the allocation of
 * the underlying array in the {@link FixedBitSet}, this
 * approach requires a number of "disk seeks" in proportion
 * to the number of terms, which can be exceptionally costly
 * when there are cache misses in the OS's IO cache.
 *
 * <p>
 * Generally, this filter will be slower on the first
 * invocation for a given field, but subsequent invocations,
 * even if you change the allowed set of Terms, should be
 * faster than TermsQuery, especially as the number of
 * Terms being matched increases.  If you are matching only
 * a very small number of terms, and those terms in turn
 * match a very small number of documents, TermsQuery may
 * perform faster.
 *
 * <p>
 * Which query is best is very application dependent.
 */
public class DocValuesTermsQuery extends Query {

  private final String field;
  private final BytesRef[] terms;

  public DocValuesTermsQuery(String field, Collection<BytesRef> terms) {
    this.field = Objects.requireNonNull(field);
    this.terms = terms.toArray(new BytesRef[terms.size()]);
    ArrayUtil.timSort(this.terms, BytesRef.getUTF8SortedAsUnicodeComparator());
  }

  public DocValuesTermsQuery(String field, BytesRef... terms) {
    this(field, Arrays.asList(terms));
  }

  public DocValuesTermsQuery(String field, final String... terms) {
    this(field, new AbstractList<BytesRef>() {
      @Override
      public BytesRef get(int index) {
        return new BytesRef(terms[index]);
      }
      @Override
      public int size() {
        return terms.length;
      }
    });
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DocValuesTermsQuery == false) {
      return false;
    }
    DocValuesTermsQuery that = (DocValuesTermsQuery) obj;
    if (!field.equals(that.field)) {
      return false;
    }
    if (getBoost() != that.getBoost()) {
      return false;
    }
    return Arrays.equals(terms, that.terms);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, Arrays.asList(terms), getBoost());
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder sb = new StringBuilder();
    sb.append(field).append(": [");
    for (BytesRef term : terms) {
      sb.append(term).append(", ");
    }
    if (terms.length > 0) {
      sb.setLength(sb.length() - 2);
    }
    return sb.append(']').toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
    return new ConstantScoreWeight(this) {

      @Override
      Scorer scorer(LeafReaderContext context, final Bits acceptDocs, final float score) throws IOException {
        final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
        final LongBitSet bits = new LongBitSet(values.getValueCount());
        for (BytesRef term : terms) {
          final long ord = values.lookupTerm(term);
          if (ord >= 0) {
            bits.set(ord);
          }
        }

        final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator() {
          @Override
          public DocIdSetIterator approximation() {
            return approximation;
          }
          @Override
          public boolean matches() throws IOException {
            final int doc = approximation.docID();
            if (acceptDocs != null && acceptDocs.get(doc) == false) {
              return false;
            }
            values.setDocument(doc);
            for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
              if (bits.get(ord)) {
                return true;
              }
            }
            return false;
          }
        };
        final DocIdSetIterator disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);
        return new Scorer(this) {

          @Override
          public TwoPhaseIterator asTwoPhaseIterator() {
            return twoPhaseIterator;
          }

          @Override
          public IntervalIterator intervals(boolean collectIntervals) throws IOException {
            return null;
          }

          @Override
          public float score() throws IOException {
            return score;
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return disi.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return disi.advance(target);
          }

          @Override
          public long cost() {
            return disi.cost();
          }

        };
      }

    };
  }

}
