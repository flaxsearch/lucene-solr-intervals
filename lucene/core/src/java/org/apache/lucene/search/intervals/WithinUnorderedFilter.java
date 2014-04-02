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

public class WithinUnorderedFilter implements IntervalFilter {

  private final WithinIntervalFilter innerFilter;
  private final boolean collectLeaves;

  /**
   * Constructs a new WithinOrderedFilter with a given slop
   * @param slop The maximum distance allowed between subintervals
   */
  public WithinUnorderedFilter(int slop, boolean collectLeaves) {
    this.innerFilter = new WithinIntervalFilter(slop);
    this.collectLeaves = collectLeaves;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return innerFilter.filter(collectIntervals,
        new ConjunctionIntervalIterator(iter.scorer, collectIntervals, collectLeaves, iter));
  }

  @Override
  public String toString() {
    return "WithinUnorderedFilter[" + this.innerFilter.getSlop() + "]";
  }
}
