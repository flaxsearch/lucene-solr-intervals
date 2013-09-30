package org.apache.lucene.search.intervals;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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

public class TestFieldedQueries {

  @Test
  public void testCreatesFieldedBooleans() {

    TermQuery q1 = new TermQuery(new Term("field", "text"));
    TermQuery q2 = new TermQuery(new Term("field", "text2"));
    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, BooleanClause.Occur.MUST);
    bq1.add(q2, BooleanClause.Occur.SHOULD);
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term("field", "text3")), BooleanClause.Occur.SHOULD);

    FieldedQuery newQ = FieldedBooleanQuery.toFieldedQuery(bq2);

    assertEquals("field", newQ.getField());

  }

  @Test
  public void testCannotCreateMixedFieldedBoolean() {

    TermQuery q1 = new TermQuery(new Term("field", "text"));
    TermQuery q2 = new TermQuery(new Term("field2", "text2"));    // This causes the error
    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(q1, BooleanClause.Occur.MUST);
    bq1.add(q2, BooleanClause.Occur.SHOULD);
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(bq1, BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term("field", "text3")), BooleanClause.Occur.SHOULD);

    try {
      FieldedBooleanQuery.toFieldedQuery(bq2);
      fail("Expected an IllegalArgumentException");
    }
    catch (Exception e) {
      assertEquals(IllegalArgumentException.class, e.getClass());
    }

  }

}
