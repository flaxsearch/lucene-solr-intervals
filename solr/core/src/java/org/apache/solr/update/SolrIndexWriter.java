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

package org.apache.solr.update;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
 * @since solr 0.9
 */

public class SolrIndexWriter extends IndexWriter {
  private static Logger log = LoggerFactory.getLogger(SolrIndexWriter.class);
  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();

  /** Stored into each Lucene commit to record the
   *  System.currentTimeMillis() when commit was called. */
  public static final String COMMIT_TIME_MSEC_KEY = "commitTimeMSec";

  String name;
  private DirectoryFactory directoryFactory;

  public static SolrIndexWriter create(String name, String path, DirectoryFactory directoryFactory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec) throws IOException {

    SolrIndexWriter w = null;
    final Directory d = directoryFactory.get(path, DirContext.DEFAULT, config.lockType);
    try {
      w = new SolrIndexWriter(name, path, d, create, schema, 
                              config, delPolicy, codec);
      w.setDirectoryFactory(directoryFactory);
      return w;
    } finally {
      if (null == w && null != d) { 
        directoryFactory.doneWithDirectory(d);
        directoryFactory.release(d);
      }
    }
  }

  private SolrIndexWriter(String name, String path, Directory directory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec) throws IOException {
    super(directory,
          config.toIndexWriterConfig(schema).
          setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND).
          setIndexDeletionPolicy(delPolicy).setCodec(codec)
          );
    log.debug("Opened Writer " + name);
    this.name = name;
    numOpens.incrementAndGet();
  }
  
  private void setDirectoryFactory(DirectoryFactory factory) {
    this.directoryFactory = factory;
  }

  /**
   * use DocumentBuilder now...
   * private final void addField(Document doc, String name, String val) {
   * SchemaField ftype = schema.getField(name);
   * <p/>
   * // we don't check for a null val ourselves because a solr.FieldType
   * // might actually want to map it to something.  If createField()
   * // returns null, then we don't store the field.
   * <p/>
   * Field field = ftype.createField(val, boost);
   * if (field != null) doc.add(field);
   * }
   * <p/>
   * <p/>
   * public void addRecord(String[] fieldNames, String[] fieldValues) throws IOException {
   * Document doc = new Document();
   * for (int i=0; i<fieldNames.length; i++) {
   * String name = fieldNames[i];
   * String val = fieldNames[i];
   * <p/>
   * // first null is end of list.  client can reuse arrays if they want
   * // and just write a single null if there is unused space.
   * if (name==null) break;
   * <p/>
   * addField(doc,name,val);
   * }
   * addDocument(doc);
   * }
   * ****
   */
  private volatile boolean isClosed = false;

  
  @Override
  public void close() throws IOException {
    log.debug("Closing Writer " + name);
    Directory directory = getDirectory();
    final InfoStream infoStream = isClosed ? null : getConfig().getInfoStream();
    try {
      while (true) {
        try {
          super.close();
        } catch (ThreadInterruptedException e) {
          // don't allow interruption
          continue;
        } catch (Throwable t) {
          if (t instanceof OutOfMemoryError) {
            throw (OutOfMemoryError) t;
          }
          log.error("Error closing IndexWriter, trying rollback", t);
          super.rollback();
        }
        if (IndexWriter.isLocked(directory)) {
          try {
            IndexWriter.unlock(directory);
          } catch (Exception e) {
            log.error("Coud not unlock directory after seemingly failed IndexWriter#close()", e);
          }
        }
        break;
      }
    } finally {
      if (infoStream != null) {
        infoStream.close();
      }
      isClosed = true;
      directoryFactory.release(directory);
      numCloses.incrementAndGet();
    }
  }

  @Override
  public void rollback() throws IOException {
    Directory dir = getDirectory();
    try {
      while (true) {
        try {
          super.rollback();
        } catch (ThreadInterruptedException e) {
          // don't allow interruption
          continue;
        }
        break;
      }
    } finally {
      isClosed = true;
      directoryFactory.release(dir);
      numCloses.incrementAndGet();
    }
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if(!isClosed){
        assert false : "SolrIndexWriter was not closed prior to finalize()";
        log.error("SolrIndexWriter was not closed prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!");
        close();
      }
    } finally { 
      super.finalize();
    }
    
  }
}
