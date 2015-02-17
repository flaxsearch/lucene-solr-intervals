package org.apache.lucene.search.intervals;

/**
 * Copyright (c) 2012 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.Query;

/**
 * A query that matches if a set of subqueries also match, and are within
 * a given distance of each other within the document.  The subqueries
 * may appear in the document in any order.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the LOWPASS<sub>k</sub> operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */

public class UnorderedNearQuery extends IntervalFilterQuery {

  private final int slop;

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param collectLeaves false if only the parent interval should be collected
   * @param subqueries the subqueries to match.
   */
  public UnorderedNearQuery(int slop, boolean collectLeaves, Query... subqueries) {
    super(createFieldConjunction(subqueries), new WithinUnorderedFilter(slop + subqueries.length - 2, collectLeaves));
    this.slop = slop;
  }

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param subqueries the subqueries to match.
   */
  public UnorderedNearQuery(int slop, Query... subqueries) {
    this(slop, true, subqueries);
  }

  @Override
  public String toString(String field) {
    return "UnorderedNear/" + slop + ":" + super.toString(field);
  }

  public static class WithinUnorderedFilter implements IntervalFilter {

    final IntervalFilter innerFilter;
    final boolean collectLeaves;

    public WithinUnorderedFilter(int slop, boolean collectLeaves) {
      this.innerFilter = new WithinIntervalFilter(slop);
      this.collectLeaves = collectLeaves;
    }

    @Override
    public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
      return innerFilter.filter(collectIntervals,
          new ConjunctionIntervalIterator(iter.scorer, collectIntervals, collectLeaves, iter.subs(false)));
    }
  }

}

