package org.apache.lucene.search.intervals;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

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
public class FieldedBooleanQuery extends FieldedQuery {

  private final BooleanQuery bq;

  public FieldedBooleanQuery(BooleanQuery bq) {
    super(extractFieldName(bq));
    this.bq = bq;
  }

  public FieldedBooleanQuery(String field, BooleanQuery bq) {
    super(field);
    this.bq = bq;
  }

  public static String extractFieldName(BooleanQuery bq) {
    String field = null;
    for (BooleanClause clause : bq.getClauses()) {
      FieldedQuery fq = toFieldedQuery(clause.getQuery());
      if (field == null)
        field = fq.getField();
      if (!field.equals(fq.getField())) {
        throw new IllegalArgumentException("Cannot create single-field boolean query from mixed-field subqueries, "
            + "found fields [" + field + "] and [" + fq.getField() + "]");
      }
    }
    return field;
  }

  @Override
  public String toString(String field) {
    return bq.toString(field);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return bq.rewrite(reader);
  }

  public static FieldedQuery toFieldedQuery(Query q) {
    if (q instanceof FieldedQuery)
      return (FieldedQuery) q;
    if (q instanceof BooleanQuery)
      return new FieldedBooleanQuery((BooleanQuery) q);
    throw new IllegalArgumentException("Cannot create FieldedQuery from query type " + q.getClass().getName());
  }

}
