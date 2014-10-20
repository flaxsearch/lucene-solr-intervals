package org.apache.lucene.search.intervals;
/**
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

public class TestBlockIntervalIterator extends IntervalTestBase {
  
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
              + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge cold! Pease porridge hot! Pease porridge in the pot nine days old! Some like it cold, some"
              + " like it hot, Some like it in the pot nine days old! Pease porridge cold! Pease porridge hot!",
          TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }

  public void testMatchingBlockIntervalFilter() throws IOException {

    Query query = makeAndQuery(
            new TermQuery(new Term("field", "pease")),
            new TermQuery(new Term("field", "porridge")),
            new TermQuery(new Term("field", "hot!"))
    );
    IntervalFilterQuery filterQuery = new IntervalFilterQuery(query, new BlockIntervalFilter(false));

    checkIntervals(filterQuery, searcher, new int[][]{
        { 0, 0, 2, 31, 33 },
        { 1, 3, 5, 34, 36 }
    });

  }

  public void testPartialMatchingBlockIntervalFilter() throws IOException {

    Query query = makeAndQuery(
          new TermQuery(new Term("field", "pease")),
          new TermQuery(new Term("field", "porridge")),
          new TermQuery(new Term("field", "hot!")),
          new TermQuery(new Term("field", "pease")),
          new TermQuery(new Term("field", "porridge")),
          new TermQuery(new Term("field", "cold!"))
    );
    IntervalFilterQuery filterQuery = new IntervalFilterQuery(query, new BlockIntervalFilter(false));

    checkIntervals(filterQuery, searcher, new int[][]{
        { 0, 0, 5, 31, 36 },
    });


  }

  public void testNonMatchingBlockIntervalFilter() throws IOException {

    Query query = makeAndQuery(
            new TermQuery(new Term("field", "pease")),
            new TermQuery(new Term("field", "hot!"))
    );
    IntervalFilterQuery filterQuery = new IntervalFilterQuery(query, new BlockIntervalFilter());

    checkIntervals(filterQuery, searcher, new int[][]{});

  }


}
