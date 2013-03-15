package org.apache.lucene.search.intervals;

import org.apache.lucene.search.Scorer;

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

/**
 * IntervalIterator based on minimum interval semantics for the Brouwerian
 * operator. This {@link IntervalIterator} computes the difference <tt>M-S</tt>
 * between the anti-chains M (minuend) and S (subtracted).
 * <p>
 * 
 * 
 * See <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 */
public class BrouwerianIntervalIterator extends IntervalIterator {
  
  private final IntervalIterator minuend;
  private final IntervalIterator subtracted;
  private Interval subtractedInterval;
  private Interval currentInterval;

  /**
   * Construct a new BrouwerianIntervalIterator over a minuend and a subtrahend
   * IntervalIterator
   * @param scorer the parent Scorer
   * @param collectIntervals true if intervals will be collected
   * @param minuend the minuend IntervalIterator
   * @param subtracted the subtrahend IntervalIterator
   */
  public BrouwerianIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator minuend, IntervalIterator subtracted) {
    super(scorer, collectIntervals);
    this.minuend = minuend;
    this.subtracted = subtracted;
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    //System.out.println("Advancing to doc " + docId);
    //Thread.dumpStack();
    minuend.scorerAdvanced(docId);
    if (subtracted.docID() <= docId)
      subtracted.scorerAdvanced(docId);
    subtractedInterval = Interval.INFINITE_INTERVAL;
    return docId;
  }
  
  @Override
  public Interval next() throws IOException {
    //System.out.println("next()");
    //System.out.println("Subtractend: " + subtracted.toString());
    //System.out.println("Minuend: " + minuend.toString());
    if (subtracted.docID() != minuend.docID() || subtractedInterval == null) {
      //System.out.println("No subtrahend on doc " + minuend.docID());
      currentInterval = minuend.next();
      //System.out.println("----Returning " + currentInterval);
      return currentInterval;
    }
    while ((currentInterval = minuend.next()) != null) {
      //System.out.println("next() : advancing through minuend");
      //System.out.println("Subtractend: " + subtracted.toString());
      //System.out.println("Current interval: " + currentInterval.toString());
      while(subtractedInterval.lessThanExclusive(currentInterval) && (subtractedInterval = subtracted.next()) != null) {
        //System.out.println("next{} : advancing through subtrahend");
        //System.out.println("Subtractend: " + subtracted.toString());
        //System.out.println("Minuend: " + minuend.toString());
      }
      if (subtractedInterval == null || !currentInterval.overlaps(subtractedInterval)) {
        //System.out.println("----Returning " + currentInterval);
        return currentInterval;
      }
    }
    //System.out.println("----Returning " + currentInterval);
    return currentInterval;
  }
  
  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    collector.collectComposite(scorer, currentInterval, docID());
    minuend.collect(collector);
    
  }
  
  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return new IntervalIterator[] {minuend, subtracted};
  }


  @Override
  public int matchDistance() {
    return minuend.matchDistance();
  }

  @Override
  public int docID() {
    return minuend.docID();
  }
  
}
