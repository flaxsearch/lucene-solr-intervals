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
    doc.add(newField("field3", "An extra field warm!", fieldType));
    writer.addDocument(doc);
  }

  // field1:and(pease, or(porridge, cold))
  @Test
  public void testNestedBooleanOnOneField() throws Exception {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "porridge")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field1", "cold!")), BooleanClause.Occur.SHOULD);
    BooleanQuery pbq = new BooleanQuery();
    pbq.add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.MUST);
    pbq.add(bq, BooleanClause.Occur.MUST);
    checkFieldIntervals(pbq, searcher, new Object[][]{
        { 0, "field1", 0, 0, "field1", 1, 1, "field1", 3, 3, "field1", 4, 4, "field1", 5, 5 },
        { 1, "field1", 0, 0, "field1", 1, 1, "field1", 3, 3, "field1", 4, 4 }
    });
  }

  @Test
  public void testSimpleBooleanOnTwoFields() throws IOException {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "warm!")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field2", "warm!")), BooleanClause.Occur.MUST);
    checkFieldIntervals(bq, searcher, new Object[][]{
        { 1, "field1", 2, 2, "field2", 3, 3 }
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

  @Test
  public void testEquivalentPositionsOnSeparateFieldsConjunctionOfDisjunction() throws IOException {

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field2", "some")), BooleanClause.Occur.SHOULD);

    BooleanQuery superq = new BooleanQuery();
    superq.add(bq, BooleanClause.Occur.MUST);
    superq.add(new TermQuery(new Term("field2", "like")), BooleanClause.Occur.MUST);

    checkFieldIntervals(superq, searcher, new Object[][]{
        {0, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 1, 1, "field2", 4, 4, "field2", 5, 5},
        {1, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 1, 1, "field2", 4, 4, "field2", 5, 5},
    });
  }

  @Test
  public void testThirdField() throws IOException {

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field1", "pease")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field2", "some")), BooleanClause.Occur.SHOULD);

    BooleanQuery superbq = new BooleanQuery();
    superbq.add(bq, BooleanClause.Occur.MUST);
    superbq.add(new TermQuery(new Term("field3", "an")), BooleanClause.Occur.MUST);

    checkFieldIntervals(superbq, searcher, new Object[][]{
        { 1, "field1", 0, 0, "field1", 3, 3, "field2", 0, 0, "field2", 4, 4, "field3", 0, 0 }
    });
  }

}
