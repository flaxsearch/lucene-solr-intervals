package org.apache.lucene.search.intervals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.junit.Ignore;

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

public class TestOldPhraseQueries extends IntervalTestBase {

  // These are all ignored at the moment, because they require multiple postingsenums
  // to work, which is a bit rubbish, frankly...

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newField(
        "field",
        "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
            + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
        TextField.TYPE_NOT_STORED));
    writer.addDocument(doc);
  }

  @Ignore
  public void testExactPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "hot!"));
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 1, 1, 2, 2, 31, 33, 31, 31, 32, 32, 33, 33 }
    });
  }

  @Ignore
  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "hot!"));
    query.setSlop(1);
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 2, 2, 31, 33, 31, 31, 33, 33 }
    });
  }

  @Ignore
  public void testManyTermSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "pot"));
    query.setSlop(2);
    checkIntervals(query, searcher, new int[][]{
        { 0, 6, 10, 6, 6, 7, 7, 10, 10 }
    });
  }

  @Ignore
  public void testMultiTermPhraseQuery() throws IOException {
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term[] {new Term("field", "hot!"), new Term("field", "cold!")});
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 1, 1, 2, 2,
             3, 5, 3, 3, 4, 4, 5, 5,
             31, 33, 31, 31, 32, 32, 33, 33,
             34, 36, 34, 34, 35, 35, 36, 36 }
    });
  }
}

