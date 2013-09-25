package org.apache.lucene.search.intervals;

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

import org.apache.lucene.search.FieldedQuery;

/**
 * A query that matches if a set of subqueries also match, and are within
 * a given distance of each other within the document.  The subqueries
 * must appear in the document in order.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the AND&lt; operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */

public class OrderedNearQuery extends IntervalFilterQuery {

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param collectLeaves false if only the master interval should be collected
   * @param subqueries the subqueries to match.
   */
  public OrderedNearQuery(int slop, boolean collectLeaves, FieldedQuery... subqueries) {
    this(slop, collectLeaves, new FieldedConjunctionQuery(subqueries));
  }

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param subqueries the subqueries to match.
   */
  public OrderedNearQuery(int slop, FieldedQuery... subqueries) {
    this(slop, true, new FieldedConjunctionQuery(subqueries));
  }

  public OrderedNearQuery(int slop, boolean collectLeaves, FieldedConjunctionQuery subqueries) {
    super(subqueries, new WithinOrderedFilter(subqueries.getField(), slop, collectLeaves));
  }

}
