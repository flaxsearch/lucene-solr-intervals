package org.apache.lucene.search.intervals;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;

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

public class FieldedConjunctionQuery extends FieldedQuery {

  private final BooleanQuery bq = new BooleanQuery();
  private final int queryCount;

  public FieldedConjunctionQuery(FieldedQuery... queries) {
    super(queries[0].getField());
    bq.add(queries[0], BooleanClause.Occur.MUST);
    for (int i = 1; i < queries.length; i++) {
      if (queries[i].getField() != queries[0].getField())
        throw new IllegalArgumentException("All subqueries must have the same field");
      bq.add(queries[i], BooleanClause.Occur.MUST);
    }
    queryCount = queries.length;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = bq.rewrite(reader);
    if (rewritten == bq)
      return this;
    return FieldedBooleanQuery.toFieldedQuery(rewritten);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return bq.createWeight(searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    bq.extractTerms(terms);
  }

  @Override
  public String toString(String field) {
    return bq.toString();
  }

  public int queryCount() {
    return queryCount;
  }
}
