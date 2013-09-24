package org.apache.lucene.index;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40RWCodec;
import org.apache.lucene.codecs.lucene41.Lucene41RWCodec;
import org.apache.lucene.codecs.lucene42.Lucene42RWCodec;
import org.apache.lucene.codecs.lucene45.Lucene45Codec;
import org.apache.lucene.codecs.lucene45.Lucene45DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Test;

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

@SuppressCodecs({"Lucene40","Lucene41","Lucene42"})
public class TestNumericDocValuesUpdates extends LuceneTestCase {
  
  private Document doc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", "doc-" + id, Store.NO));
    // make sure we don't set the doc's value to 0, to not confuse with a document that's missing values
    doc.add(new NumericDocValuesField("val", id + 1));
    return doc;
  }
  
  @Test
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(10);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0)); // val=1
    writer.addDocument(doc(1)); // val=2
    if (random().nextBoolean()) { // randomly commit before the update is sent
      writer.commit();
    }
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 2L); // doc=0, exp=2
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer, true);
      writer.close();
    }
    
    assertEquals(1, reader.leaves().size());
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("val");
    assertEquals(2, ndv.get(0));
    assertEquals(2, ndv.get(1));
    reader.close();
    
    dir.close();
  }
  
  @Test
  public void testUpdateFewSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(2); // generate few segments
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES); // prevent merges for this test
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = 10;
    long[] expectedValues = new long[numDocs];
    for (int i = 0; i < numDocs; i++) {
      writer.addDocument(doc(i));
      expectedValues[i] = i + 1;
    }
    writer.commit();
    
    // update few docs
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() < 0.4) {
        long value = (i + 1) * 2;
        writer.updateNumericDocValue(new Term("id", "doc-" + i), "val", value);
        expectedValues[i] = value;
      }
    }
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer, true);
      writer.close();
    }
    
    for (AtomicReaderContext context : reader.leaves()) {
      AtomicReader r = context.reader();
      NumericDocValues ndv = r.getNumericDocValues("val");
      assertNotNull(ndv);
      for (int i = 0; i < r.maxDoc(); i++) {
        long expected = expectedValues[i + context.docBase];
        long actual = ndv.get(i);
        assertEquals(expected, actual);
      }
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testReopen() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    final boolean isNRT = random().nextBoolean();
    final DirectoryReader reader1;
    if (isNRT) {
      reader1 = DirectoryReader.open(writer, true);
    } else {
      writer.commit();
      reader1 = DirectoryReader.open(dir);
    }
    
    // update doc
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 10L); // update doc-0's value to 10
    if (!isNRT) {
      writer.commit();
    }
    
    // reopen reader and assert only it sees the update
    final DirectoryReader reader2 = DirectoryReader.openIfChanged(reader1);
    assertNotNull(reader2);
    assertTrue(reader1 != reader2);
    
    assertEquals(1, reader1.leaves().get(0).reader().getNumericDocValues("val").get(0));
    assertEquals(10, reader2.leaves().get(0).reader().getNumericDocValues("val").get(0));
    
    IOUtils.close(writer, reader1, reader2, dir);
  }
  
  @Test
  public void testUpdatesAndDeletes() throws Exception {
    // create an index with a segment with only deletes, a segment with both
    // deletes and updates and a segment with only updates
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // control segment flushing
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES); // prevent merges for this test
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 6; i++) {
      writer.addDocument(doc(i));
      if (i % 2 == 1) {
        writer.commit(); // create 2-docs segments
      }
    }
    
    // delete doc-1 and doc-2
    writer.deleteDocuments(new Term("id", "doc-1"), new Term("id", "doc-2")); // 1st and 2nd segments
    
    // update docs 3 and 5
    writer.updateNumericDocValue(new Term("id", "doc-3"), "val", 17L);
    writer.updateNumericDocValue(new Term("id", "doc-5"), "val", 17L);
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer, true);
      writer.close();
    }
    
    AtomicReader slow = SlowCompositeReaderWrapper.wrap(reader);
    
    Bits liveDocs = slow.getLiveDocs();
    boolean[] expectedLiveDocs = new boolean[] { true, false, false, true, true, true };
    for (int i = 0; i < expectedLiveDocs.length; i++) {
      assertEquals(expectedLiveDocs[i], liveDocs.get(i));
    }
    
    long[] expectedValues = new long[] { 1, 2, 3, 17, 5, 17};
    NumericDocValues ndv = slow.getNumericDocValues("val");
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], ndv.get(i));
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testUpdatesWithDeletes() throws Exception {
    // update and delete different documents in the same commit session
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // control segment flushing
    IndexWriter writer = new IndexWriter(dir, conf);
    
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    writer.deleteDocuments(new Term("id", "doc-0"));
    writer.updateNumericDocValue(new Term("id", "doc-1"), "val", 17L);
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer, true);
      writer.close();
    }
    
    AtomicReader r = reader.leaves().get(0).reader();
    assertFalse(r.getLiveDocs().get(0));
    assertEquals(17, r.getNumericDocValues("val").get(1));
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testUpdateAndDeleteSameDocument() throws Exception {
    // update and delete same document in same commit session
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // control segment flushing
    IndexWriter writer = new IndexWriter(dir, conf);
    
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    writer.deleteDocuments(new Term("id", "doc-0"));
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 17L);
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer, true);
      writer.close();
    }
    
    AtomicReader r = reader.leaves().get(0).reader();
    assertFalse(r.getLiveDocs().get(0));
    assertEquals(1, r.getNumericDocValues("val").get(0)); // deletes are currently applied first
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testMultipleDocValuesTypes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // prevent merges
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      doc.add(new NumericDocValuesField("ndv", i));
      doc.add(new BinaryDocValuesField("bdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedDocValuesField("sdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(Integer.toString(i * 2))));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' ndv field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    BinaryDocValues bdv = r.getBinaryDocValues("bdv");
    SortedDocValues sdv = r.getSortedDocValues("sdv");
    SortedSetDocValues ssdv = r.getSortedSetDocValues("ssdv");
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, ndv.get(0));
      bdv.get(i, scratch);
      assertEquals(new BytesRef(Integer.toString(i)), scratch);
      sdv.get(i, scratch);
      assertEquals(new BytesRef(Integer.toString(i)), scratch);
      ssdv.setDocument(i);
      long ord = ssdv.nextOrd();
      ssdv.lookupOrd(ord, scratch);
      assertEquals(i, Integer.parseInt(scratch.utf8ToString()));
      if (i != 0) {
        ord = ssdv.nextOrd();
        ssdv.lookupOrd(ord, scratch);
        assertEquals(i * 2, Integer.parseInt(scratch.utf8ToString()));
      }
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, ssdv.nextOrd());
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testMultipleNumericDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // prevent merges
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      doc.add(new NumericDocValuesField("ndv1", i));
      doc.add(new NumericDocValuesField("ndv2", i));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' ndv1 field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv1", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv1 = r.getNumericDocValues("ndv1");
    NumericDocValues ndv2 = r.getNumericDocValues("ndv2");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, ndv1.get(i));
      assertEquals(i, ndv2.get(i));
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testDocumentWithNoValue() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      if (i == 0) { // index only one document with value
        doc.add(new NumericDocValuesField("ndv", 5));
      }
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' ndv field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, ndv.get(i));
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testUnsetValue() throws Exception {
    assumeTrue("codec does not support docsWithField", defaultCodecSupportsDocsWithField());
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "doc" + i, Store.NO));
      doc.add(new NumericDocValuesField("ndv", 5));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // unset the value of 'doc0'
    writer.updateNumericDocValue(new Term("id", "doc0"), "ndv", null);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      if (i == 0) {
        assertEquals(0, ndv.get(i));
      } else {
        assertEquals(5, ndv.get(i));
      }
    }
    
    Bits docsWithField = r.getDocsWithField("ndv");
    assertFalse(docsWithField.get(0));
    assertTrue(docsWithField.get(1));
    
    reader.close();
    dir.close();
  }
  
  public void testUnsetAllValues() throws Exception {
    assumeTrue("codec does not support docsWithField", defaultCodecSupportsDocsWithField());
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "doc", Store.NO));
      doc.add(new NumericDocValuesField("ndv", 5));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // unset the value of 'doc'
    writer.updateNumericDocValue(new Term("id", "doc"), "ndv", null);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(0, ndv.get(i));
    }
    
    Bits docsWithField = r.getDocsWithField("ndv");
    assertFalse(docsWithField.get(0));
    assertFalse(docsWithField.get(1));
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testUpdateNonDocValueField() throws Exception {
    // we don't support adding new fields or updating existing non-numeric-dv
    // fields through numeric updates
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new StringField("foo", "bar", Store.NO));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    try {
      writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L);
      fail("should not have allowed creating new fields through update");
    } catch (IllegalArgumentException e) {
      // ok
    }
    
    try {
      writer.updateNumericDocValue(new Term("key", "doc"), "foo", 17L);
      fail("should not have allowed updating an existing field to numeric-dv");
    } catch (IllegalArgumentException e) {
      // ok
    }
    
    writer.close();
    dir.close();
  }
  
  @Test
  public void testDifferentDVFormatPerField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setCodec(new Lucene45Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new Lucene45DocValuesFormat();
      }
    });
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("value")));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    
    AtomicReader r = SlowCompositeReaderWrapper.wrap(reader);
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    SortedDocValues sdv = r.getSortedDocValues("sorted");
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, ndv.get(i));
      sdv.get(i, scratch);
      assertEquals(new BytesRef("value"), scratch);
    }
    
    reader.close();
    dir.close();
  }
  
  @Test
  public void testUpdateSameDocMultipleTimes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L); // update existing field
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 3L); // update existing field 2nd time in this commit
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    final AtomicReader r = SlowCompositeReaderWrapper.wrap(reader);
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(3, ndv.get(i));
    }
    reader.close();
    dir.close();
  }
  
  @Test
  public void testSegmentMerges() throws Exception {
    Directory dir = newDirectory();
    Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    IndexWriter writer = new IndexWriter(dir, conf.clone());
    
    int docid = 0;
    int numRounds = atLeast(10);
    for (int rnd = 0; rnd < numRounds; rnd++) {
      Document doc = new Document();
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new NumericDocValuesField("ndv", -1));
      int numDocs = atLeast(30);
      for (int i = 0; i < numDocs; i++) {
        doc.removeField("id");
        doc.add(new StringField("id", Integer.toString(docid++), Store.NO));
        writer.addDocument(doc);
      }
      
      long value = rnd + 1;
      writer.updateNumericDocValue(new Term("key", "doc"), "ndv", value);
      
      if (random.nextDouble() < 0.2) { // randomly delete some docs
        writer.deleteDocuments(new Term("id", Integer.toString(random.nextInt(docid))));
      }
      
      // randomly commit or reopen-IW (or nothing), before forceMerge
      if (random.nextDouble() < 0.4) {
        writer.commit();
      } else if (random.nextDouble() < 0.1) {
        writer.close();
        writer = new IndexWriter(dir, conf.clone());
      }

      // add another document with the current value, to be sure forceMerge has
      // something to merge (for instance, it could be that CMS finished merging
      // all segments down to 1 before the delete was applied, so when
      // forceMerge is called, the index will be with one segment and deletes
      // and some MPs might now merge it, thereby invalidating test's
      // assumption that the reader has no deletes).
      doc = new Document();
      doc.add(new StringField("id", Integer.toString(docid++), Store.NO));
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new NumericDocValuesField("ndv", value));
      writer.addDocument(doc);

      writer.forceMerge(1, true);
      final DirectoryReader reader;
      if (random.nextBoolean()) {
        writer.commit();
        reader = DirectoryReader.open(dir);
      } else {
        reader = DirectoryReader.open(writer, true);
      }
      
      assertEquals(1, reader.leaves().size());
      final AtomicReader r = reader.leaves().get(0).reader();
      assertNull("index should have no deletes after forceMerge", r.getLiveDocs());
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      assertNotNull(ndv);
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(value, ndv.get(i));
      }
      reader.close();
    }
    
    writer.close();
    dir.close();
  }
  
  @Test
  public void testUpdateDocumentByMultipleTerms() throws Exception {
    // make sure the order of updates is respected, even when multiple terms affect same document
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("k1", "v1", Store.NO));
    doc.add(new StringField("k2", "v2", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("k1", "v1"), "ndv", 17L);
    writer.updateNumericDocValue(new Term("k2", "v2"), "ndv", 3L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    final AtomicReader r = SlowCompositeReaderWrapper.wrap(reader);
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(3, ndv.get(i));
    }
    reader.close();
    dir.close();
  }
  
  @Test
  public void testManyReopensAndFields() throws Exception {
    Directory dir = newDirectory();
    final Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    LogMergePolicy lmp = newLogMergePolicy();
    lmp.setMergeFactor(3); // merge often
    conf.setMergePolicy(lmp);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    final boolean isNRT = random.nextBoolean();
    DirectoryReader reader;
    if (isNRT) {
      reader = DirectoryReader.open(writer, true);
    } else {
      writer.commit();
      reader = DirectoryReader.open(dir);
    }
    
    final int numFields = random.nextInt(4) + 3; // 3-7
    final long[] fieldValues = new long[numFields];
    for (int i = 0; i < fieldValues.length; i++) {
      fieldValues[i] = 1;
    }
    
    int numRounds = atLeast(15);
    int docID = 0;
    for (int i = 0; i < numRounds; i++) {
      int numDocs = atLeast(5);
//      System.out.println("[" + Thread.currentThread().getName() + "]: round=" + i + ", numDocs=" + numDocs);
      for (int j = 0; j < numDocs; j++) {
        Document doc = new Document();
        doc.add(new StringField("id", "doc-" + docID, Store.NO));
        doc.add(new StringField("key", "all", Store.NO)); // update key
        // add all fields with their current (updated value)
        for (int f = 0; f < fieldValues.length; f++) {
          doc.add(new NumericDocValuesField("f" + f, fieldValues[f]));
        }
        writer.addDocument(doc);
        ++docID;
      }
      
      int fieldIdx = random.nextInt(fieldValues.length);
      String updateField = "f" + fieldIdx;
      writer.updateNumericDocValue(new Term("key", "all"), updateField, ++fieldValues[fieldIdx]);
//      System.out.println("[" + Thread.currentThread().getName() + "]: updated field '" + updateField + "' to value " + fieldValues[fieldIdx]);
      
      if (random.nextDouble() < 0.2) {
        int deleteDoc = random.nextInt(docID); // might also delete an already deleted document, ok!
        writer.deleteDocuments(new Term("id", "doc-" + deleteDoc));
//        System.out.println("[" + Thread.currentThread().getName() + "]: deleted document: doc-" + deleteDoc);
      }
      
      // verify reader
      if (!isNRT) {
        writer.commit();
      }
      
//      System.out.println("[" + Thread.currentThread().getName() + "]: reopen reader: " + reader);
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
//      System.out.println("[" + Thread.currentThread().getName() + "]: reopened reader: " + reader);
      assertTrue(reader.numDocs() > 0); // we delete at most one document per round
      for (AtomicReaderContext context : reader.leaves()) {
        AtomicReader r = context.reader();
//        System.out.println(((SegmentReader) r).getSegmentName());
        Bits liveDocs = r.getLiveDocs();
        for (int field = 0; field < fieldValues.length; field++) {
          String f = "f" + field;
          NumericDocValues ndv = r.getNumericDocValues(f);
          assertNotNull(ndv);
          int maxDoc = r.maxDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs == null || liveDocs.get(doc)) {
              //              System.out.println("doc=" + (doc + context.docBase) + " f='" + f + "' vslue=" + ndv.get(doc));
              assertEquals("invalid value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], ndv.get(doc));
            }
          }
        }
      }
//      System.out.println();
    }
    
    IOUtils.close(writer, reader, dir);
  }
  
  @Test
  public void testUpdateSegmentWithNoDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // prevent merges, otherwise by the time updates are applied
    // (writer.close()), the segments might have merged and that update becomes
    // legit.
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // first segment with NDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no NDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the second segment
    writer.updateNumericDocValue(new Term("id", "doc1"), "ndv", 5L);
    try {
      writer.close();
      fail("should not have succeeded updating a segment with no numeric DocValues field");
    } catch (UnsupportedOperationException e) {
      // expected
      writer.rollback();
    }
    
    dir.close();
  }
  
  @Test
  public void testUpdateSegmentWithPostingButNoDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // prevent merges, otherwise by the time updates are applied
    // (writer.close()), the segments might have merged and that update becomes
    // legit.
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // first segment with NDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new StringField("ndv", "mock-value", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no NDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    doc.add(new StringField("ndv", "mock-value", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    
    // update documentin the second segment
    writer.updateNumericDocValue(new Term("id", "doc1"), "ndv", 5L);
    try {
      writer.close();
      fail("should not have succeeded updating a segment with no numeric DocValues field");
    } catch (UnsupportedOperationException e) {
      // expected
      writer.rollback();
    }
    
    dir.close();
  }
  
  @Test
  public void testUpdateNumericDVFieldWithSameNameAsPostingField() throws Exception {
    // this used to fail because FieldInfos.Builder neglected to update
    // globalFieldMaps.docValueTypes map
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("f", "mock-value", Store.NO));
    doc.add(new NumericDocValuesField("f", 5));
    writer.addDocument(doc);
    writer.commit();
    writer.updateNumericDocValue(new Term("f", "mock-value"), "f", 17L);
    writer.close();
    
    DirectoryReader r = DirectoryReader.open(dir);
    NumericDocValues ndv = r.leaves().get(0).reader().getNumericDocValues("f");
    assertEquals(17, ndv.get(0));
    r.close();
    
    dir.close();
  }
  
  @Test
  public void testUpdateOldSegments() throws Exception {
    Codec[] oldCodecs = new Codec[] { new Lucene40RWCodec(), new Lucene41RWCodec(), new Lucene42RWCodec() };
    Directory dir = newDirectory();
    
    // create a segment with an old Codec
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setCodec(oldCodecs[random().nextInt(oldCodecs.length)]);
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "doc", Store.NO));
    doc.add(new NumericDocValuesField("f", 5));
    writer.addDocument(doc);
    writer.close();
    
    conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    writer = new IndexWriter(dir, conf);
    writer.updateNumericDocValue(new Term("id", "doc"), "f", 4L);
    OLD_FORMAT_IMPERSONATION_IS_ACTIVE = false;
    try {
      writer.close();
      fail("should not have succeeded to update a segment written with an old Codec");
    } catch (UnsupportedOperationException e) {
      writer.rollback(); 
    } finally {
      OLD_FORMAT_IMPERSONATION_IS_ACTIVE = true;
    }
    
    dir.close();
  }
  
  @Test
  public void testStressMultiThreading() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    final IndexWriter writer = new IndexWriter(dir, conf);
    
    // create index
    final int numThreads = atLeast(3);
    final int numDocs = atLeast(2000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      double group = random().nextDouble();
      String g;
      if (group < 0.1) g = "g0";
      else if (group < 0.5) g = "g1";
      else if (group < 0.8) g = "g2";
      else g = "g3";
      doc.add(new StringField("updKey", g, Store.NO));
      for (int j = 0; j < numThreads; j++) {
        long value = random().nextInt();
        doc.add(new NumericDocValuesField("f" + j, value));
        doc.add(new NumericDocValuesField("cf" + j, value * 2)); // control, always updated to f * 2
      }
      writer.addDocument(doc);
    }
    
    final CountDownLatch done = new CountDownLatch(numThreads);
    
    // same thread updates a field as well as reopens
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      final String f = "f" + i;
      final String cf = "cf" + i;
      final int numThreadUpdates = atLeast(40);
      threads[i] = new Thread("UpdateThread-" + i) {
        @Override
        public void run() {
          try {
            Random random = random();
            int numUpdates = numThreadUpdates;
            while (numUpdates-- > 0) {
              double group = random.nextDouble();
              Term t;
              if (group < 0.1) t = new Term("updKey", "g0");
              else if (group < 0.5) t = new Term("updKey", "g1");
              else if (group < 0.8) t = new Term("updKey", "g2");
              else t = new Term("updKey", "g3");
//              System.out.println("[" + Thread.currentThread().getName() + "] numUpdates=" + numUpdates + " updateTerm=" + t);
              long updValue = random.nextInt();
              writer.updateNumericDocValue(t, f, updValue);
              writer.updateNumericDocValue(t, cf, updValue * 2);
              
              if (random.nextDouble() < 0.2) {
                // delete a random document
                int doc = random.nextInt(numDocs);
                writer.deleteDocuments(new Term("id", "doc" + doc));
              }
              
              if (random.nextDouble() < 0.1) {
                writer.commit(); // rarely commit
              }
              
              if (random.nextDouble() < 0.3) { // obtain NRT reader (apply updates)
                DirectoryReader.open(writer, true).close();
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          } finally {
            done.countDown();
          }
        }
      };
    }
    
    for (Thread t : threads) t.start();
    done.await();
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    for (AtomicReaderContext context : reader.leaves()) {
      AtomicReader r = context.reader();
      for (int i = 0; i < numThreads; i++) {
        NumericDocValues ndv = r.getNumericDocValues("f" + i);
        NumericDocValues control = r.getNumericDocValues("cf" + i);
        Bits liveDocs = r.getLiveDocs();
        for (int j = 0; j < r.maxDoc(); j++) {
          if (liveDocs == null || liveDocs.get(j)) {
            assertEquals(control.get(j), ndv.get(j) * 2);
          }
        }
      }
    }
    reader.close();
    
    dir.close();
  }

  @Test
  public void testUpdateDifferentDocsInDifferentGens() throws Exception {
    // update same document multiple times across generations
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(4);
    IndexWriter writer = new IndexWriter(dir, conf);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "doc" + i, Store.NO));
      long value = random().nextInt();
      doc.add(new NumericDocValuesField("f", value));
      doc.add(new NumericDocValuesField("cf", value * 2));
      writer.addDocument(doc);
    }
    
    int numGens = atLeast(5);
    for (int i = 0; i < numGens; i++) {
      int doc = random().nextInt(numDocs);
      Term t = new Term("id", "doc" + doc);
      long value = random().nextLong();
      writer.updateNumericDocValue(t, "f", value);
      writer.updateNumericDocValue(t, "cf", value * 2);
      DirectoryReader reader = DirectoryReader.open(writer, true);
      for (AtomicReaderContext context : reader.leaves()) {
        AtomicReader r = context.reader();
        NumericDocValues fndv = r.getNumericDocValues("f");
        NumericDocValues cfndv = r.getNumericDocValues("cf");
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(cfndv.get(j), fndv.get(j) * 2);
        }
      }
      reader.close();
    }
    writer.close();
    dir.close();
  }

  @Test
  public void testChangeCodec() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setCodec(new Lucene45Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new Lucene45DocValuesFormat();
      }
    });
    IndexWriter writer = new IndexWriter(dir, conf.clone());
    Document doc = new Document();
    doc.add(new StringField("id", "d0", Store.NO));
    doc.add(new NumericDocValuesField("f1", 5L));
    doc.add(new NumericDocValuesField("f2", 13L));
    writer.addDocument(doc);
    writer.close();
    
    // change format
    conf.setCodec(new Lucene45Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new AssertingDocValuesFormat();
      }
    });
    writer = new IndexWriter(dir, conf.clone());
    doc = new Document();
    doc.add(new StringField("id", "d1", Store.NO));
    doc.add(new NumericDocValuesField("f1", 17L));
    doc.add(new NumericDocValuesField("f2", 2L));
    writer.addDocument(doc);
    writer.updateNumericDocValue(new Term("id", "d0"), "f1", 12L);
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader r = SlowCompositeReaderWrapper.wrap(reader);
    NumericDocValues f1 = r.getNumericDocValues("f1");
    NumericDocValues f2 = r.getNumericDocValues("f2");
    assertEquals(12L, f1.get(0));
    assertEquals(13L, f2.get(0));
    assertEquals(17L, f1.get(1));
    assertEquals(2L, f2.get(1));
    reader.close();
    dir.close();
  }

}
