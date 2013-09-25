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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

public class FieldedDisjunctionQuery extends FieldedQuery {

  private final BooleanQuery bq = new BooleanQuery();
  private final int queryCount;

  public FieldedDisjunctionQuery(FieldedQuery... queries) {
    super(queries[0].getField());
    bq.add(queries[0], BooleanClause.Occur.SHOULD);
    for (int i = 1; i < queries.length; i++) {
      if (queries[i].getField() != queries[0].getField())
        throw new IllegalArgumentException("All subqueries must have the same field");
      bq.add(queries[i], BooleanClause.Occur.SHOULD);
    }
    queryCount = queries.length;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return bq.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    return bq.toString();
  }

  public int queryCount() {
    return queryCount;
  }
}
