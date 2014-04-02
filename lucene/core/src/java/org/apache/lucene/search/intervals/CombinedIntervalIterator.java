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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

public class CombinedIntervalIterator extends IntervalIterator {

  private final IntervalPriorityQueue intervalQueue;
  private final IntervalIterator[] children;

  public CombinedIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator... children) {
    super(scorer, collectIntervals);
    this.children = children;
    intervalQueue = new IntervalPriorityQueue(children.length);
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    intervalQueue.clear();
    for (IntervalIterator child : children) {
      IntervalIteratorRef ref = new IntervalIteratorRef(child, docId);
      if (ref.interval != null)
        intervalQueue.add(ref);
    }
    intervalQueue.updateTop();
    return docId;
  }

  @Override
  public Interval next() throws IOException {
    if (intervalQueue.size() == 0)
      return null;

    do {
      IntervalIteratorRef top = intervalQueue.top();
      if (top.buffered) {
        top.buffered = false;
        return top.interval;
      }

      Interval interval;
      if ((interval = top.iterator.next()) != null) {
        top.interval = interval;
        intervalQueue.updateTop();
        return interval;
      }
      else
        intervalQueue.pop();
    } while (intervalQueue.size() > 0);
    return null;
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    for (IntervalIterator iterator : children) {
      iterator.collect(collector);
    }
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return children;
  }

  @Override
  public int matchDistance() {
    return 0;
  }

  public static class IntervalIteratorRef {

    final IntervalIterator iterator;
    Interval interval;
    int doc = -1;
    boolean buffered = false;

    public IntervalIteratorRef(IntervalIterator iterator, int advanceTo) throws IOException {
      this.iterator = iterator;
      this.doc = this.iterator.scorerAdvanced(advanceTo);
      if (this.doc == advanceTo) {
        this.interval = this.iterator.next();
        this.buffered = true;
      }
    }
  }

  public static class IntervalPriorityQueue extends PriorityQueue<IntervalIteratorRef> {

    public IntervalPriorityQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(IntervalIteratorRef a, IntervalIteratorRef b) {
      return a.doc < b.doc || a.doc == b.doc && a.interval.strictlyLessThan(b.interval);
    }
  }
}
