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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * Basic equivalence tests for approximations.
 */
public class TestApproximationSearchEquivalence extends SearchEquivalenceTestBase {
  
  public void testConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    assertSameScores(bq1, bq2);
  }

  public void testNestedConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, Occur.MUST);
    bq2.add(q3, Occur.MUST);
    
    BooleanQuery bq3 = new BooleanQuery();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    BooleanQuery bq4 = new BooleanQuery();
    bq4.add(bq3, Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2, bq4);
  }

  public void testDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    assertSameScores(bq1, bq2);
  }

  public void testNestedDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, Occur.SHOULD);
    bq2.add(q3, Occur.SHOULD);

    BooleanQuery bq3 = new BooleanQuery();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    BooleanQuery bq4 = new BooleanQuery();
    bq4.add(bq3, Occur.SHOULD);
    bq4.add(q3, Occur.SHOULD);

    assertSameScores(bq2, bq4);
  }

  public void testDisjunctionInConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, Occur.MUST);
    bq2.add(q3, Occur.MUST);

    BooleanQuery bq3 = new BooleanQuery();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    BooleanQuery bq4 = new BooleanQuery();
    bq4.add(bq3, Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2, bq4);
  }

  public void testConjunctionInDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, Occur.SHOULD);
    bq2.add(q3, Occur.SHOULD);

    BooleanQuery bq3 = new BooleanQuery();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    BooleanQuery bq4 = new BooleanQuery();
    bq4.add(bq3, Occur.SHOULD);
    bq4.add(q3, Occur.SHOULD);

    assertSameScores(bq2, bq4);
  }
}
