package org.apache.lucene.search.intervals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

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

public class TestFieldedIntervals extends IntervalTestBase {

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();
    doc.add(newField("field1", "Pease porridge hot! Pease porridge cold!", fieldType));
    doc.add(newField("field2", "Some like it hot!  Some like it cold", fieldType));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newField("field1", "Pease porridge warm! Pease porridge tepid!", fieldType));
    doc.add(newField("field2", "Some like it warm!  Some like it tepid", fieldType));
    writer.addDocument(doc);
  }

  @Test
  public void testSimpleBooleanOnTwoFields() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "hot!")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field2", "hot!")), BooleanClause.Occur.MUST);
    checkFieldIntervals(bq, searcher, new Object[][]{
        { 0, "field1", 2, 2, "field2", 3, 3, }
    });
  }

  @Test
  public void testSimpleBooleanOnDisjointFields() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "hot!")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field2", "warm!")), BooleanClause.Occur.SHOULD);
    checkFieldIntervals(bq, searcher, new Object[][]{
        { 0, "field1", 2, 2 },
        { 1, "field2", 3, 3 }
    });
  }

  @Test
  public void testEquivalentPositionsOnSeparateFieldsDisjunction() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field2", "some")), BooleanClause.Occur.SHOULD);
    checkFieldIntervals(bq, searcher, new Object[][]{
        { 0, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 4, 4 },
        { 1, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 4, 4 },
    });
  }

  @Test
  public void testEquivalentPositionsOnSeparateFieldsConjunction() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field2", "some")), BooleanClause.Occur.MUST);
    checkFieldIntervals(bq, searcher, new Object[][]{
        { 0, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 4, 4 },
        { 1, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 4, 4 },
    });
  }


}
