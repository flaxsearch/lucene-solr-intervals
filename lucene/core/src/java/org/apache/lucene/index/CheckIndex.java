package org.apache.lucene.index;

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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CheckIndex.Status.DocValuesStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CommandLineUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * Basic tool and API to check the health of an index and
 * write a new segments file that removes reference to
 * problematic segments.
 * 
 * <p>As this tool checks every byte in the index, on a large
 * index it can take quite a long time to run.
 *
 * @lucene.experimental Please make a complete backup of your
 * index before using this to exorcise corrupted documents from your index!
 */
public class CheckIndex implements Closeable {

  private PrintStream infoStream;
  private Directory dir;
  private Lock writeLock;
  private volatile boolean closed;

  /**
   * Returned from {@link #checkIndex()} detailing the health and status of the index.
   *
   * @lucene.experimental
   **/

  public static class Status {

    Status() {
    }

    /** True if no problems were found with the index. */
    public boolean clean;

    /** True if we were unable to locate and load the segments_N file. */
    public boolean missingSegments;

    /** True if we were unable to open the segments_N file. */
    public boolean cantOpenSegments;

    /** True if we were unable to read the version number from segments_N file. */
    public boolean missingSegmentVersion;

    /** Name of latest segments_N file in the index. */
    public String segmentsFileName;

    /** Number of segments in the index. */
    public int numSegments;

    /** Empty unless you passed specific segments list to check as optional 3rd argument.
     *  @see CheckIndex#checkIndex(List) */
    public List<String> segmentsChecked = new ArrayList<>();
  
    /** True if the index was created with a newer version of Lucene than the CheckIndex tool. */
    public boolean toolOutOfDate;

    /** List of {@link SegmentInfoStatus} instances, detailing status of each segment. */
    public List<SegmentInfoStatus> segmentInfos = new ArrayList<>();
  
    /** Directory index is in. */
    public Directory dir;

    /** 
     * SegmentInfos instance containing only segments that
     * had no problems (this is used with the {@link CheckIndex#exorciseIndex} 
     * method to repair the index. 
     */
    SegmentInfos newSegments;

    /** How many documents will be lost to bad segments. */
    public int totLoseDocCount;

    /** How many bad segments were found. */
    public int numBadSegments;

    /** True if we checked only specific segments ({@link
     * #checkIndex(List)}) was called with non-null
     * argument). */
    public boolean partial;

    /** The greatest segment name. */
    public int maxSegmentName;

    /** Whether the SegmentInfos.counter is greater than any of the segments' names. */
    public boolean validCounter; 

    /** Holds the userData of the last commit in the index */
    public Map<String, String> userData;

    /** Holds the status of each segment in the index.
     *  See {@link #segmentInfos}.
     *
     * @lucene.experimental
     */
    public static class SegmentInfoStatus {

      SegmentInfoStatus() {
      }

      /** Name of the segment. */
      public String name;

      /** Codec used to read this segment. */
      public Codec codec;

      /** Document count (does not take deletions into account). */
      public int docCount;

      /** True if segment is compound file format. */
      public boolean compound;

      /** Number of files referenced by this segment. */
      public int numFiles;

      /** Net size (MB) of the files referenced by this
       *  segment. */
      public double sizeMB;

      /** True if this segment has pending deletions. */
      public boolean hasDeletions;

      /** Current deletions generation. */
      public long deletionsGen;

      /** True if we were able to open an LeafReader on this
       *  segment. */
      public boolean openReaderPassed;

      /** Map that includes certain
       *  debugging details that IndexWriter records into
       *  each segment it creates */
      public Map<String,String> diagnostics;
      
      /** Status for testing of livedocs */
      public LiveDocStatus liveDocStatus;
      
      /** Status for testing of field infos */
      public FieldInfoStatus fieldInfoStatus;

      /** Status for testing of field norms (null if field norms could not be tested). */
      public FieldNormStatus fieldNormStatus;

      /** Status for testing of indexed terms (null if indexed terms could not be tested). */
      public TermIndexStatus termIndexStatus;

      /** Status for testing of stored fields (null if stored fields could not be tested). */
      public StoredFieldStatus storedFieldStatus;

      /** Status for testing of term vectors (null if term vectors could not be tested). */
      public TermVectorStatus termVectorStatus;
      
      /** Status for testing of DocValues (null if DocValues could not be tested). */
      public DocValuesStatus docValuesStatus;
    }
    
    /**
     * Status from testing livedocs
     */
    public static final class LiveDocStatus {
      private LiveDocStatus() {
      }
      
      /** Number of deleted documents. */
      public int numDeleted;
      
      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }
    
    /**
     * Status from testing field infos.
     */
    public static final class FieldInfoStatus {
      private FieldInfoStatus() {
      }

      /** Number of fields successfully tested */
      public long totFields = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing field norms.
     */
    public static final class FieldNormStatus {
      private FieldNormStatus() {
      }

      /** Number of fields successfully tested */
      public long totFields = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing term index.
     */
    public static final class TermIndexStatus {

      TermIndexStatus() {
      }

      /** Number of terms with at least one live doc. */
      public long termCount = 0L;

      /** Number of terms with zero live docs docs. */
      public long delTermCount = 0L;

      /** Total frequency across all terms. */
      public long totFreq = 0L;
      
      /** Total number of positions. */
      public long totPos = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;

      /** Holds details of block allocations in the block
       *  tree terms dictionary (this is only set if the
       *  {@link PostingsFormat} for this segment uses block
       *  tree. */
      public Map<String,Object> blockTreeStats = null;
    }

    /**
     * Status from testing stored fields.
     */
    public static final class StoredFieldStatus {

      StoredFieldStatus() {
      }
      
      /** Number of documents tested. */
      public int docCount = 0;
      
      /** Total number of stored fields tested. */
      public long totFields = 0;
      
      /** Exception thrown during stored fields test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing stored fields.
     */
    public static final class TermVectorStatus {
      
      TermVectorStatus() {
      }

      /** Number of documents tested. */
      public int docCount = 0;
      
      /** Total number of term vectors tested. */
      public long totVectors = 0;
      
      /** Exception thrown during term vector test (null on success) */
      public Throwable error = null;
    }
    
    /**
     * Status from testing DocValues
     */
    public static final class DocValuesStatus {

      DocValuesStatus() {
      }

      /** Total number of docValues tested. */
      public long totalValueFields;
      
      /** Total number of numeric fields */
      public long totalNumericFields;
      
      /** Total number of binary fields */
      public long totalBinaryFields;
      
      /** Total number of sorted fields */
      public long totalSortedFields;
      
      /** Total number of sortednumeric fields */
      public long totalSortedNumericFields;
      
      /** Total number of sortedset fields */
      public long totalSortedSetFields;
      
      /** Exception thrown during doc values test (null on success) */
      public Throwable error = null;
    }
  }

  /** Create a new CheckIndex on the directory. */
  public CheckIndex(Directory dir) throws IOException {
    this(dir, dir.makeLock(IndexWriter.WRITE_LOCK_NAME));
  }
  
  /** 
   * Expert: create a directory with the specified lock.
   * This should really not be used except for unit tests!!!!
   * It exists only to support special tests (such as TestIndexWriterExceptions*),
   * that would otherwise be more complicated to debug if they had to close the writer
   * for each check.
   */
  public CheckIndex(Directory dir, Lock writeLock) throws IOException {
    this.dir = dir;
    this.writeLock = writeLock;
    this.infoStream = null;
    if (!writeLock.obtain(IndexWriterConfig.WRITE_LOCK_TIMEOUT)) { // obtain write lock
      throw new LockObtainFailedException("Index locked for write: " + writeLock);
    }
  }
  
  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("this instance is closed");
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    IOUtils.close(writeLock);
  }

  private boolean crossCheckTermVectors;

  /** If true, term vectors are compared against postings to
   *  make sure they are the same.  This will likely
   *  drastically increase time it takes to run CheckIndex! */
  public void setCrossCheckTermVectors(boolean v) {
    crossCheckTermVectors = v;
  }

  /** See {@link #setCrossCheckTermVectors}. */
  public boolean getCrossCheckTermVectors() {
    return crossCheckTermVectors;
  }

  private boolean failFast;

  /** If true, just throw the original exception immediately when
   *  corruption is detected, rather than continuing to iterate to other
   *  segments looking for more corruption.  */
  public void setFailFast(boolean v) {
    failFast = v;
  }

  /** See {@link #setFailFast}. */
  public boolean getFailFast() {
    return failFast;
  }

  private boolean verbose;

  /** Set infoStream where messages should go.  If null, no
   *  messages are printed.  If verbose is true then more
   *  details are printed. */
  public void setInfoStream(PrintStream out, boolean verbose) {
    infoStream = out;
    this.verbose = verbose;
  }

  /** Set infoStream where messages should go. See {@link #setInfoStream(PrintStream,boolean)}. */
  public void setInfoStream(PrintStream out) {
    setInfoStream(out, false);
  }

  private static void msg(PrintStream out, String msg) {
    if (out != null)
      out.println(msg);
  }

  /** Returns a {@link Status} instance detailing
   *  the state of the index.
   *
   *  <p>As this method checks every byte in the index, on a large
   *  index it can take quite a long time to run.
   *
   *  <p><b>WARNING</b>: make sure
   *  you only call this when the index is not opened by any
   *  writer. */
  public Status checkIndex() throws IOException {
    return checkIndex(null);
  }
  
  /** Returns a {@link Status} instance detailing
   *  the state of the index.
   * 
   *  @param onlySegments list of specific segment names to check
   *
   *  <p>As this method checks every byte in the specified
   *  segments, on a large index it can take quite a long
   *  time to run. */
  public Status checkIndex(List<String> onlySegments) throws IOException {
    ensureOpen();
    long startNS = System.nanoTime();
    NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
    SegmentInfos sis = null;
    Status result = new Status();
    result.dir = dir;
    String[] files = dir.listAll();
    String lastSegmentsFile = SegmentInfos.getLastCommitSegmentsFileName(files);
    if (lastSegmentsFile == null) {
      throw new IndexNotFoundException("no segments* file found in " + dir + ": files: " + Arrays.toString(files));
    }
    try {
      // Do not use SegmentInfos.read(Directory) since the spooky
      // retrying it does is not necessary here (we hold the write lock):
      sis = SegmentInfos.readCommit(dir, lastSegmentsFile);
    } catch (Throwable t) {
      if (failFast) {
        IOUtils.reThrow(t);
      }
      msg(infoStream, "ERROR: could not read any segments file in directory");
      result.missingSegments = true;
      if (infoStream != null)
        t.printStackTrace(infoStream);
      return result;
    }

    // find the oldest and newest segment versions
    Version oldest = null;
    Version newest = null;
    String oldSegs = null;
    for (SegmentCommitInfo si : sis) {
      Version version = si.info.getVersion();
      if (version == null) {
        // pre-3.1 segment
        oldSegs = "pre-3.1";
      } else {
        if (oldest == null || version.onOrAfter(oldest) == false) {
          oldest = version;
        }
        if (newest == null || version.onOrAfter(newest)) {
          newest = version;
        }
      }
    }

    final int numSegments = sis.size();
    final String segmentsFileName = sis.getSegmentsFileName();
    // note: we only read the format byte (required preamble) here!
    IndexInput input = null;
    try {
      input = dir.openInput(segmentsFileName, IOContext.READONCE);
    } catch (Throwable t) {
      if (failFast) {
        IOUtils.reThrow(t);
      }
      msg(infoStream, "ERROR: could not open segments file in directory");
      if (infoStream != null)
        t.printStackTrace(infoStream);
      result.cantOpenSegments = true;
      return result;
    }
    int format = 0;
    try {
      format = input.readInt();
    } catch (Throwable t) {
      if (failFast) {
        IOUtils.reThrow(t);
      }
      msg(infoStream, "ERROR: could not read segment file version in directory");
      if (infoStream != null)
        t.printStackTrace(infoStream);
      result.missingSegmentVersion = true;
      return result;
    } finally {
      if (input != null)
        input.close();
    }

    String sFormat = "";
    boolean skip = false;

    result.segmentsFileName = segmentsFileName;
    result.numSegments = numSegments;
    result.userData = sis.getUserData();
    String userDataString;
    if (sis.getUserData().size() > 0) {
      userDataString = " userData=" + sis.getUserData();
    } else {
      userDataString = "";
    }

    String versionString = "";
    if (oldSegs != null) {
      if (newest != null) {
        versionString = "versions=[" + oldSegs + " .. " + newest + "]";
      } else {
        versionString = "version=" + oldSegs;
      }
    } else if (newest != null) { // implies oldest != null
      versionString = oldest.equals(newest) ? ( "version=" + oldest ) : ("versions=[" + oldest + " .. " + newest + "]");
    }

    msg(infoStream, "Segments file=" + segmentsFileName + " numSegments=" + numSegments
        + " " + versionString + " id=" + StringHelper.idToString(sis.getId()) + " format=" + sFormat + userDataString);

    if (onlySegments != null) {
      result.partial = true;
      if (infoStream != null) {
        infoStream.print("\nChecking only these segments:");
        for (String s : onlySegments) {
          infoStream.print(" " + s);
        }
      }
      result.segmentsChecked.addAll(onlySegments);
      msg(infoStream, ":");
    }

    if (skip) {
      msg(infoStream, "\nERROR: this index appears to be created by a newer version of Lucene than this tool was compiled on; please re-compile this tool on the matching version of Lucene; exiting");
      result.toolOutOfDate = true;
      return result;
    }


    result.newSegments = sis.clone();
    result.newSegments.clear();
    result.maxSegmentName = -1;

    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = sis.info(i);
      int segmentName = Integer.parseInt(info.info.name.substring(1), Character.MAX_RADIX);
      if (segmentName > result.maxSegmentName) {
        result.maxSegmentName = segmentName;
      }
      if (onlySegments != null && !onlySegments.contains(info.info.name)) {
        continue;
      }
      Status.SegmentInfoStatus segInfoStat = new Status.SegmentInfoStatus();
      result.segmentInfos.add(segInfoStat);
      msg(infoStream, "  " + (1+i) + " of " + numSegments + ": name=" + info.info.name + " docCount=" + info.info.getDocCount());
      segInfoStat.name = info.info.name;
      segInfoStat.docCount = info.info.getDocCount();
      
      final Version version = info.info.getVersion();
      if (info.info.getDocCount() <= 0 && version != null && version.onOrAfter(Version.LUCENE_4_5_0)) {
        throw new RuntimeException("illegal number of documents: maxDoc=" + info.info.getDocCount());
      }

      int toLoseDocCount = info.info.getDocCount();

      SegmentReader reader = null;

      try {
        msg(infoStream, "    version=" + (version == null ? "3.0" : version));
        msg(infoStream, "    id=" + StringHelper.idToString(info.info.getId()));
        final Codec codec = info.info.getCodec();
        msg(infoStream, "    codec=" + codec);
        segInfoStat.codec = codec;
        msg(infoStream, "    compound=" + info.info.getUseCompoundFile());
        segInfoStat.compound = info.info.getUseCompoundFile();
        msg(infoStream, "    numFiles=" + info.files().size());
        segInfoStat.numFiles = info.files().size();
        segInfoStat.sizeMB = info.sizeInBytes()/(1024.*1024.);
        msg(infoStream, "    size (MB)=" + nf.format(segInfoStat.sizeMB));
        Map<String,String> diagnostics = info.info.getDiagnostics();
        segInfoStat.diagnostics = diagnostics;
        if (diagnostics.size() > 0) {
          msg(infoStream, "    diagnostics = " + diagnostics);
        }

        if (!info.hasDeletions()) {
          msg(infoStream, "    no deletions");
          segInfoStat.hasDeletions = false;
        }
        else{
          msg(infoStream, "    has deletions [delGen=" + info.getDelGen() + "]");
          segInfoStat.hasDeletions = true;
          segInfoStat.deletionsGen = info.getDelGen();
        }
        
        long startOpenReaderNS = System.nanoTime();
        if (infoStream != null)
          infoStream.print("    test: open reader.........");
        reader = new SegmentReader(info, IOContext.DEFAULT);
        msg(infoStream, String.format(Locale.ROOT, "OK [took %.3f sec]", nsToSec(System.nanoTime()-startOpenReaderNS)));

        segInfoStat.openReaderPassed = true;
        
        long startIntegrityNS = System.nanoTime();
        if (infoStream != null)
          infoStream.print("    test: check integrity.....");
        reader.checkIntegrity();
        msg(infoStream, String.format(Locale.ROOT, "OK [took %.3f sec]", nsToSec(System.nanoTime()-startIntegrityNS)));

        if (reader.maxDoc() != info.info.getDocCount()) {
          throw new RuntimeException("SegmentReader.maxDoc() " + reader.maxDoc() + " != SegmentInfos.docCount " + info.info.getDocCount());
        }
        
        final int numDocs = reader.numDocs();
        toLoseDocCount = numDocs;
        
        if (reader.hasDeletions()) {
          if (reader.numDocs() != info.info.getDocCount() - info.getDelCount()) {
            throw new RuntimeException("delete count mismatch: info=" + (info.info.getDocCount() - info.getDelCount()) + " vs reader=" + reader.numDocs());
          }
          if ((info.info.getDocCount() - reader.numDocs()) > reader.maxDoc()) {
            throw new RuntimeException("too many deleted docs: maxDoc()=" + reader.maxDoc() + " vs del count=" + (info.info.getDocCount() - reader.numDocs()));
          }
          if (info.info.getDocCount() - reader.numDocs() != info.getDelCount()) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.info.getDocCount() - reader.numDocs()));
          }
        } else {
          if (info.getDelCount() != 0) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.info.getDocCount() - reader.numDocs()));
          }
        }
        
        // Test Livedocs
        segInfoStat.liveDocStatus = testLiveDocs(reader, infoStream, failFast);

        // Test Fieldinfos
        segInfoStat.fieldInfoStatus = testFieldInfos(reader, infoStream, failFast);
        
        // Test Field Norms
        segInfoStat.fieldNormStatus = testFieldNorms(reader, infoStream, failFast);

        // Test the Term Index
        segInfoStat.termIndexStatus = testPostings(reader, infoStream, verbose, failFast);

        // Test Stored Fields
        segInfoStat.storedFieldStatus = testStoredFields(reader, infoStream, failFast);

        // Test Term Vectors
        segInfoStat.termVectorStatus = testTermVectors(reader, infoStream, verbose, crossCheckTermVectors, failFast);

        segInfoStat.docValuesStatus = testDocValues(reader, infoStream, failFast);

        // Rethrow the first exception we encountered
        //  This will cause stats for failed segments to be incremented properly
        if (segInfoStat.liveDocStatus.error != null) {
          throw new RuntimeException("Live docs test failed");
        } else if (segInfoStat.fieldInfoStatus.error != null) {
          throw new RuntimeException("Field Info test failed");
        } else if (segInfoStat.fieldNormStatus.error != null) {
          throw new RuntimeException("Field Norm test failed");
        } else if (segInfoStat.termIndexStatus.error != null) {
          throw new RuntimeException("Term Index test failed");
        } else if (segInfoStat.storedFieldStatus.error != null) {
          throw new RuntimeException("Stored Field test failed");
        } else if (segInfoStat.termVectorStatus.error != null) {
          throw new RuntimeException("Term Vector test failed");
        }  else if (segInfoStat.docValuesStatus.error != null) {
          throw new RuntimeException("DocValues test failed");
        }

        msg(infoStream, "");
        
        if (verbose) {
          msg(infoStream, "detailed segment RAM usage: ");
          msg(infoStream, Accountables.toString(reader));
        }

      } catch (Throwable t) {
        if (failFast) {
          IOUtils.reThrow(t);
        }
        msg(infoStream, "FAILED");
        String comment;
        comment = "exorciseIndex() would remove reference to this segment";
        msg(infoStream, "    WARNING: " + comment + "; full exception:");
        if (infoStream != null)
          t.printStackTrace(infoStream);
        msg(infoStream, "");
        result.totLoseDocCount += toLoseDocCount;
        result.numBadSegments++;
        continue;
      } finally {
        if (reader != null)
          reader.close();
      }

      // Keeper
      result.newSegments.add(info.clone());
    }

    if (0 == result.numBadSegments) {
      result.clean = true;
    } else
      msg(infoStream, "WARNING: " + result.numBadSegments + " broken segments (containing " + result.totLoseDocCount + " documents) detected");

    if ( ! (result.validCounter = (result.maxSegmentName < sis.counter))) {
      result.clean = false;
      result.newSegments.counter = result.maxSegmentName + 1; 
      msg(infoStream, "ERROR: Next segment name counter " + sis.counter + " is not greater than max segment name " + result.maxSegmentName);
    }
    
    if (result.clean) {
      msg(infoStream, "No problems were detected with this index.\n");
    }

    msg(infoStream, String.format(Locale.ROOT, "Took %.3f sec total.", nsToSec(System.nanoTime()-startNS)));

    return result;
  }
  
  /**
   * Test live docs.
   * @lucene.experimental
   */
  public static Status.LiveDocStatus testLiveDocs(LeafReader reader, PrintStream infoStream, boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.LiveDocStatus status = new Status.LiveDocStatus();
    
    try {
      if (infoStream != null)
        infoStream.print("    test: check live docs.....");
      final int numDocs = reader.numDocs();
      if (reader.hasDeletions()) {
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
          throw new RuntimeException("segment should have deletions, but liveDocs is null");
        } else {
          int numLive = 0;
          for (int j = 0; j < liveDocs.length(); j++) {
            if (liveDocs.get(j)) {
              numLive++;
            }
          }
          if (numLive != numDocs) {
            throw new RuntimeException("liveDocs count mismatch: info=" + numDocs + ", vs bits=" + numLive);
          }
        }
        
        status.numDeleted = reader.numDeletedDocs();
        msg(infoStream, String.format(Locale.ROOT, "OK [%d deleted docs] [took %.3f sec]", status.numDeleted, nsToSec(System.nanoTime()-startNS)));
      } else {
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs != null) {
          // it's ok for it to be non-null here, as long as none are set right?
          for (int j = 0; j < liveDocs.length(); j++) {
            if (!liveDocs.get(j)) {
              throw new RuntimeException("liveDocs mismatch: info says no deletions but doc " + j + " is deleted.");
            }
          }
        }
        msg(infoStream, String.format(Locale.ROOT, "OK [took %.3f sec]", (nsToSec(System.nanoTime()-startNS))));
      }
      
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    
    return status;
  }
  
  /**
   * Test field infos.
   * @lucene.experimental
   */
  public static Status.FieldInfoStatus testFieldInfos(LeafReader reader, PrintStream infoStream, boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.FieldInfoStatus status = new Status.FieldInfoStatus();
    
    try {
      // Test Field Infos
      if (infoStream != null) {
        infoStream.print("    test: field infos.........");
      }
      FieldInfos fieldInfos = reader.getFieldInfos();
      for (FieldInfo f : fieldInfos) {
        f.checkConsistency();
      }
      msg(infoStream, String.format(Locale.ROOT, "OK [%d fields] [took %.3f sec]", fieldInfos.size(), nsToSec(System.nanoTime()-startNS)));
      status.totFields = fieldInfos.size();
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    
    return status;
  }

  /**
   * Test field norms.
   * @lucene.experimental
   */
  public static Status.FieldNormStatus testFieldNorms(LeafReader reader, PrintStream infoStream, boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.FieldNormStatus status = new Status.FieldNormStatus();

    try {
      // Test Field Norms
      if (infoStream != null) {
        infoStream.print("    test: field norms.........");
      }
      for (FieldInfo info : reader.getFieldInfos()) {
        if (info.hasNorms()) {
          checkNorms(info, reader, infoStream);
          ++status.totFields;
        } else {
          if (reader.getNormValues(info.name) != null) {
            throw new RuntimeException("field: " + info.name + " should omit norms but has them!");
          }
        }
      }

      msg(infoStream, String.format(Locale.ROOT, "OK [%d fields] [took %.3f sec]", status.totFields, nsToSec(System.nanoTime()-startNS)));
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }

  /**
   * checks Fields api is consistent with itself.
   * searcher is optional, to verify with queries. Can be null.
   */
  private static Status.TermIndexStatus checkFields(Fields fields, Bits liveDocs, int maxDoc, FieldInfos fieldInfos, boolean doPrint, boolean isVectors, PrintStream infoStream, boolean verbose) throws IOException {
    // TODO: we should probably return our own stats thing...?!
    long startNS;
    if (doPrint) {
      startNS = System.nanoTime();
    } else {
      startNS = 0;
    }
    
    final Status.TermIndexStatus status = new Status.TermIndexStatus();
    int computedFieldCount = 0;
    
    PostingsEnum docs = null;
    PostingsEnum docsAndFreqs = null;
    PostingsEnum postings = null;
    
    String lastField = null;
    for (String field : fields) {
      // MultiFieldsEnum relies upon this order...
      if (lastField != null && field.compareTo(lastField) <= 0) {
        throw new RuntimeException("fields out of order: lastField=" + lastField + " field=" + field);
      }
      lastField = field;
      
      // check that the field is in fieldinfos, and is indexed.
      // TODO: add a separate test to check this for different reader impls
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        throw new RuntimeException("fieldsEnum inconsistent with fieldInfos, no fieldInfos for: " + field);
      }
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        throw new RuntimeException("fieldsEnum inconsistent with fieldInfos, isIndexed == false for: " + field);
      }
      
      // TODO: really the codec should not return a field
      // from FieldsEnum if it has no Terms... but we do
      // this today:
      // assert fields.terms(field) != null;
      computedFieldCount++;
      
      final Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }
      
      final boolean hasFreqs = terms.hasFreqs();
      final boolean hasPositions = terms.hasPositions();
      final boolean hasPayloads = terms.hasPayloads();
      final boolean hasOffsets = terms.hasOffsets();
      
      BytesRef maxTerm;
      BytesRef minTerm;
      if (isVectors) {
        // Term vectors impls can be very slow for getMax
        maxTerm = null;
        minTerm = null;
      } else {
        BytesRef bb = terms.getMin();
        if (bb != null) {
          assert bb.isValid();
          minTerm = BytesRef.deepCopyOf(bb);
        } else {
          minTerm = null;
        }

        bb = terms.getMax();
        if (bb != null) {
          assert bb.isValid();
          maxTerm = BytesRef.deepCopyOf(bb);
          if (minTerm == null) {
            throw new RuntimeException("field \"" + field + "\" has null minTerm but non-null maxTerm");
          }
        } else {
          maxTerm = null;
          if (minTerm != null) {
            throw new RuntimeException("field \"" + field + "\" has non-null minTerm but null maxTerm");
          }
        }
      }

      // term vectors cannot omit TF:
      final boolean expectedHasFreqs = (isVectors || fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0);

      if (hasFreqs != expectedHasFreqs) {
        throw new RuntimeException("field \"" + field + "\" should have hasFreqs=" + expectedHasFreqs + " but got " + hasFreqs);
      }

      if (hasFreqs == false) {
        if (terms.getSumTotalTermFreq() != -1) {
          throw new RuntimeException("field \"" + field + "\" hasFreqs is false, but Terms.getSumTotalTermFreq()=" + terms.getSumTotalTermFreq() + " (should be -1)");
        }
      }

      if (!isVectors) {
        final boolean expectedHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        if (hasPositions != expectedHasPositions) {
          throw new RuntimeException("field \"" + field + "\" should have hasPositions=" + expectedHasPositions + " but got " + hasPositions);
        }

        final boolean expectedHasPayloads = fieldInfo.hasPayloads();
        if (hasPayloads != expectedHasPayloads) {
          throw new RuntimeException("field \"" + field + "\" should have hasPayloads=" + expectedHasPayloads + " but got " + hasPayloads);
        }

        final boolean expectedHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        if (hasOffsets != expectedHasOffsets) {
          throw new RuntimeException("field \"" + field + "\" should have hasOffsets=" + expectedHasOffsets + " but got " + hasOffsets);
        }
      }

      final TermsEnum termsEnum = terms.iterator(null);

      boolean hasOrd = true;
      final long termCountStart = status.delTermCount + status.termCount;
      
      BytesRefBuilder lastTerm = null;
      
      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      long upto = 0;
      FixedBitSet visitedDocs = new FixedBitSet(maxDoc);
      while(true) {
        
        final BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }

        assert term.isValid();
        
        // make sure terms arrive in order according to
        // the comp
        if (lastTerm == null) {
          lastTerm = new BytesRefBuilder();
          lastTerm.copyBytes(term);
        } else {
          if (lastTerm.get().compareTo(term) >= 0) {
            throw new RuntimeException("terms out of order: lastTerm=" + lastTerm + " term=" + term);
          }
          lastTerm.copyBytes(term);
        }

        if (isVectors == false) {
          if (minTerm == null) {
            // We checked this above:
            assert maxTerm == null;
            throw new RuntimeException("field=\"" + field + "\": invalid term: term=" + term + ", minTerm=" + minTerm);
          }
        
          if (term.compareTo(minTerm) < 0) {
            throw new RuntimeException("field=\"" + field + "\": invalid term: term=" + term + ", minTerm=" + minTerm);
          }
        
          if (term.compareTo(maxTerm) > 0) {
            throw new RuntimeException("field=\"" + field + "\": invalid term: term=" + term + ", maxTerm=" + maxTerm);
          }
        }
        
        final int docFreq = termsEnum.docFreq();
        if (docFreq <= 0) {
          throw new RuntimeException("docfreq: " + docFreq + " is out of bounds");
        }
        sumDocFreq += docFreq;
        
        docs = termsEnum.postings(liveDocs, docs);
        postings = termsEnum.postings(liveDocs, postings, PostingsEnum.ALL);

        if (hasFreqs == false) {
          if (termsEnum.totalTermFreq() != -1) {
            throw new RuntimeException("field \"" + field + "\" hasFreqs is false, but TermsEnum.totalTermFreq()=" + termsEnum.totalTermFreq() + " (should be -1)");   
          }
        }
        
        if (hasOrd) {
          long ord = -1;
          try {
            ord = termsEnum.ord();
          } catch (UnsupportedOperationException uoe) {
            hasOrd = false;
          }
          
          if (hasOrd) {
            final long ordExpected = status.delTermCount + status.termCount - termCountStart;
            if (ord != ordExpected) {
              throw new RuntimeException("ord mismatch: TermsEnum has ord=" + ord + " vs actual=" + ordExpected);
            }
          }
        }
        
        final PostingsEnum docs2;
        if (postings != null) {
          docs2 = postings;
        } else {
          docs2 = docs;
        }
        
        int lastDoc = -1;
        int docCount = 0;
        long totalTermFreq = 0;
        while(true) {
          final int doc = docs2.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          status.totFreq++;
          visitedDocs.set(doc);
          int freq = -1;
          if (hasFreqs) {
            freq = docs2.freq();
            if (freq <= 0) {
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " is out of bounds");
            }
            status.totPos += freq;
            totalTermFreq += freq;
          } else {
            // When a field didn't index freq, it must
            // consistently "lie" and pretend that freq was
            // 1:
            if (docs2.freq() != 1) {
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " != 1 when Terms.hasFreqs() is false");
            }
          }
          docCount++;
          
          if (doc <= lastDoc) {
            throw new RuntimeException("term " + term + ": doc " + doc + " <= lastDoc " + lastDoc);
          }
          if (doc >= maxDoc) {
            throw new RuntimeException("term " + term + ": doc " + doc + " >= maxDoc " + maxDoc);
          }
          
          lastDoc = doc;
          
          int lastPos = -1;
          int lastOffset = 0;
          if (hasPositions) {
            for(int j=0;j<freq;j++) {
              final int pos = postings.nextPosition();

              if (pos < 0) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " is out of bounds");
              }
              if (pos < lastPos) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " < lastPos " + lastPos);
              }
              lastPos = pos;
              BytesRef payload = postings.getPayload();
              if (payload != null) {
                assert payload.isValid();
              }
              if (payload != null && payload.length < 1) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " payload length is out of bounds " + payload.length);
              }
              if (hasOffsets) {
                int startOffset = postings.startOffset();
                int endOffset = postings.endOffset();
                // NOTE: we cannot enforce any bounds whatsoever on vectors... they were a free-for-all before?
                // but for offsets in the postings lists these checks are fine: they were always enforced by IndexWriter
                if (!isVectors) {
                  if (startOffset < 0) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": startOffset " + startOffset + " is out of bounds");
                  }
                  if (startOffset < lastOffset) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": startOffset " + startOffset + " < lastStartOffset " + lastOffset);
                  }
                  if (endOffset < 0) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": endOffset " + endOffset + " is out of bounds");
                  }
                  if (endOffset < startOffset) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": endOffset " + endOffset + " < startOffset " + startOffset);
                  }
                }
                lastOffset = startOffset;
              }
            }
          }
        }
        
        if (docCount != 0) {
          status.termCount++;
        } else {
          status.delTermCount++;
        }
        
        final long totalTermFreq2 = termsEnum.totalTermFreq();
        final boolean hasTotalTermFreq = hasFreqs && totalTermFreq2 != -1;
        
        // Re-count if there are deleted docs:
        if (liveDocs != null) {
          if (hasFreqs) {
            final PostingsEnum docsNoDel = termsEnum.postings(null, docsAndFreqs);
            docCount = 0;
            totalTermFreq = 0;
            while(docsNoDel.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              visitedDocs.set(docsNoDel.docID());
              docCount++;
              totalTermFreq += docsNoDel.freq();
            }
          } else {
            final PostingsEnum docsNoDel = termsEnum.postings(null, docs, PostingsEnum.NONE);
            docCount = 0;
            totalTermFreq = -1;
            while(docsNoDel.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              visitedDocs.set(docsNoDel.docID());
              docCount++;
            }
          }
        }
        
        if (docCount != docFreq) {
          throw new RuntimeException("term " + term + " docFreq=" + docFreq + " != tot docs w/o deletions " + docCount);
        }
        if (hasTotalTermFreq) {
          if (totalTermFreq2 <= 0) {
            throw new RuntimeException("totalTermFreq: " + totalTermFreq2 + " is out of bounds");
          }
          sumTotalTermFreq += totalTermFreq;
          if (totalTermFreq != totalTermFreq2) {
            throw new RuntimeException("term " + term + " totalTermFreq=" + totalTermFreq2 + " != recomputed totalTermFreq=" + totalTermFreq);
          }
        }
        
        // Test skipping
        if (hasPositions) {
          for(int idx=0;idx<7;idx++) {
            final int skipDocID = (int) (((idx+1)*(long) maxDoc)/8);
            postings = termsEnum.postings(liveDocs, postings, PostingsEnum.ALL);
            final int docID = postings.advance(skipDocID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            } else {
              if (docID < skipDocID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + ") returned docID=" + docID);
              }
              final int freq = postings.freq();
              if (freq <= 0) {
                throw new RuntimeException("termFreq " + freq + " is out of bounds");
              }
              int lastPosition = -1;
              int lastOffset = 0;
              for(int posUpto=0;posUpto<freq;posUpto++) {
                final int pos = postings.nextPosition();

                if (pos < 0) {
                  throw new RuntimeException("position " + pos + " is out of bounds");
                }
                if (pos < lastPosition) {
                  throw new RuntimeException("position " + pos + " is < lastPosition " + lastPosition);
                }
                lastPosition = pos;
                if (hasOffsets) {
                  int startOffset = postings.startOffset();
                  int endOffset = postings.endOffset();
                  // NOTE: we cannot enforce any bounds whatsoever on vectors... they were a free-for-all before?
                  // but for offsets in the postings lists these checks are fine: they were always enforced by IndexWriter
                  if (!isVectors) {
                    if (startOffset < 0) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": startOffset " + startOffset + " is out of bounds");
                    }
                    if (startOffset < lastOffset) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": startOffset " + startOffset + " < lastStartOffset " + lastOffset);
                    }
                    if (endOffset < 0) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": endOffset " + endOffset + " is out of bounds");
                    }
                    if (endOffset < startOffset) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": endOffset " + endOffset + " < startOffset " + startOffset);
                    }
                  }
                  lastOffset = startOffset;
                }
              } 
              
              final int nextDocID = postings.nextDoc();
              if (nextDocID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              if (nextDocID <= docID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + "), then .next() returned docID=" + nextDocID + " vs prev docID=" + docID);
              }
            }

            if (isVectors) {
              // Only 1 doc in the postings for term vectors, so we only test 1 advance:
              break;
            }
          }
        } else {
          for(int idx=0;idx<7;idx++) {
            final int skipDocID = (int) (((idx+1)*(long) maxDoc)/8);
            docs = termsEnum.postings(liveDocs, docs, PostingsEnum.NONE);
            final int docID = docs.advance(skipDocID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            } else {
              if (docID < skipDocID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + ") returned docID=" + docID);
              }
              final int nextDocID = docs.nextDoc();
              if (nextDocID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              if (nextDocID <= docID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + "), then .next() returned docID=" + nextDocID + " vs prev docID=" + docID);
              }
            }
            if (isVectors) {
              // Only 1 doc in the postings for term vectors, so we only test 1 advance:
              break;
            }
          }
        }
      }
      
      if (minTerm != null && status.termCount + status.delTermCount == 0) {
        throw new RuntimeException("field=\"" + field + "\": minTerm is non-null yet we saw no terms: " + minTerm);
      }

      final Terms fieldTerms = fields.terms(field);
      if (fieldTerms == null) {
        // Unusual: the FieldsEnum returned a field but
        // the Terms for that field is null; this should
        // only happen if it's a ghost field (field with
        // no terms, eg there used to be terms but all
        // docs got deleted and then merged away):
        
      } else {
        final Object stats = fieldTerms.getStats();
        assert stats != null;
        if (status.blockTreeStats == null) {
          status.blockTreeStats = new HashMap<>();
        }
        status.blockTreeStats.put(field, stats);
        
        if (sumTotalTermFreq != 0) {
          final long v = fields.terms(field).getSumTotalTermFreq();
          if (v != -1 && sumTotalTermFreq != v) {
            throw new RuntimeException("sumTotalTermFreq for field " + field + "=" + v + " != recomputed sumTotalTermFreq=" + sumTotalTermFreq);
          }
        }
        
        if (sumDocFreq != 0) {
          final long v = fields.terms(field).getSumDocFreq();
          if (v != -1 && sumDocFreq != v) {
            throw new RuntimeException("sumDocFreq for field " + field + "=" + v + " != recomputed sumDocFreq=" + sumDocFreq);
          }
        }
        
        if (fieldTerms != null) {
          final int v = fieldTerms.getDocCount();
          if (v != -1 && visitedDocs.cardinality() != v) {
            throw new RuntimeException("docCount for field " + field + "=" + v + " != recomputed docCount=" + visitedDocs.cardinality());
          }
        }
        
        // Test seek to last term:
        if (lastTerm != null) {
          if (termsEnum.seekCeil(lastTerm.get()) != TermsEnum.SeekStatus.FOUND) { 
            throw new RuntimeException("seek to last term " + lastTerm + " failed");
          }
          
          int expectedDocFreq = termsEnum.docFreq();
          PostingsEnum d = termsEnum.postings(null, null, PostingsEnum.NONE);
          int docFreq = 0;
          while (d.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            docFreq++;
          }
          if (docFreq != expectedDocFreq) {
            throw new RuntimeException("docFreq for last term " + lastTerm + "=" + expectedDocFreq + " != recomputed docFreq=" + docFreq);
          }
        }
        
        // check unique term count
        long termCount = -1;
        
        if ((status.delTermCount+status.termCount)-termCountStart > 0) {
          termCount = fields.terms(field).size();
          
          if (termCount != -1 && termCount != status.delTermCount + status.termCount - termCountStart) {
            throw new RuntimeException("termCount mismatch " + (status.delTermCount + termCount) + " vs " + (status.termCount - termCountStart));
          }
        }
        
        // Test seeking by ord
        if (hasOrd && status.termCount-termCountStart > 0) {
          int seekCount = (int) Math.min(10000L, termCount);
          if (seekCount > 0) {
            BytesRef[] seekTerms = new BytesRef[seekCount];
            
            // Seek by ord
            for(int i=seekCount-1;i>=0;i--) {
              long ord = i*(termCount/seekCount);
              termsEnum.seekExact(ord);
              seekTerms[i] = BytesRef.deepCopyOf(termsEnum.term());
            }
            
            // Seek by term
            long totDocCount = 0;
            for(int i=seekCount-1;i>=0;i--) {
              if (termsEnum.seekCeil(seekTerms[i]) != TermsEnum.SeekStatus.FOUND) {
                throw new RuntimeException("seek to existing term " + seekTerms[i] + " failed");
              }
              
              docs = termsEnum.postings(liveDocs, docs, PostingsEnum.NONE);
              if (docs == null) {
                throw new RuntimeException("null DocsEnum from to existing term " + seekTerms[i]);
              }
              
              while(docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                totDocCount++;
              }
            }
            
            long totDocCountNoDeletes = 0;
            long totDocFreq = 0;
            for(int i=0;i<seekCount;i++) {
              if (!termsEnum.seekExact(seekTerms[i])) {
                throw new RuntimeException("seek to existing term " + seekTerms[i] + " failed");
              }
              
              totDocFreq += termsEnum.docFreq();
              docs = termsEnum.postings(null, docs, PostingsEnum.NONE);
              if (docs == null) {
                throw new RuntimeException("null DocsEnum from to existing term " + seekTerms[i]);
              }
              
              while(docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                totDocCountNoDeletes++;
              }
            }
            
            if (totDocCount > totDocCountNoDeletes) {
              throw new RuntimeException("more postings with deletes=" + totDocCount + " than without=" + totDocCountNoDeletes);
            }
            
            if (totDocCountNoDeletes != totDocFreq) {
              throw new RuntimeException("docfreqs=" + totDocFreq + " != recomputed docfreqs=" + totDocCountNoDeletes);
            }
          }
        }
      }
    }
    
    int fieldCount = fields.size();
    
    if (fieldCount != -1) {
      if (fieldCount < 0) {
        throw new RuntimeException("invalid fieldCount: " + fieldCount);
      }
      if (fieldCount != computedFieldCount) {
        throw new RuntimeException("fieldCount mismatch " + fieldCount + " vs recomputed field count " + computedFieldCount);
      }
    }

    if (doPrint) {
      msg(infoStream, String.format(Locale.ROOT, "OK [%d terms; %d terms/docs pairs; %d tokens] [took %.3f sec]",
                                    status.termCount, status.totFreq, status.totPos, nsToSec(System.nanoTime()-startNS)));
    }
    
    if (verbose && status.blockTreeStats != null && infoStream != null && status.termCount > 0) {
      for(Map.Entry<String, Object> ent : status.blockTreeStats.entrySet()) {
        infoStream.println("      field \"" + ent.getKey() + "\":");
        infoStream.println("      " + ent.getValue().toString().replace("\n", "\n      "));
      }
    }
    
    return status;
  }

  /**
   * Test the term index.
   * @lucene.experimental
   */
  public static Status.TermIndexStatus testPostings(LeafReader reader, PrintStream infoStream) throws IOException {
    return testPostings(reader, infoStream, false, false);
  }
  
  /**
   * Test the term index.
   * @lucene.experimental
   */
  public static Status.TermIndexStatus testPostings(LeafReader reader, PrintStream infoStream, boolean verbose, boolean failFast) throws IOException {

    // TODO: we should go and verify term vectors match, if
    // crossCheckTermVectors is on...

    Status.TermIndexStatus status;
    final int maxDoc = reader.maxDoc();
    final Bits liveDocs = reader.getLiveDocs();

    try {
      if (infoStream != null) {
        infoStream.print("    test: terms, freq, prox...");
      }

      final Fields fields = reader.fields();
      final FieldInfos fieldInfos = reader.getFieldInfos();
      status = checkFields(fields, liveDocs, maxDoc, fieldInfos, true, false, infoStream, verbose);
      if (liveDocs != null) {
        if (infoStream != null) {
          infoStream.print("    test (ignoring deletes): terms, freq, prox...");
        }
        checkFields(fields, null, maxDoc, fieldInfos, true, false, infoStream, verbose);
      }
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR: " + e);
      status = new Status.TermIndexStatus();
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  /**
   * Test stored fields.
   * @lucene.experimental
   */
  public static Status.StoredFieldStatus testStoredFields(LeafReader reader, PrintStream infoStream, boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.StoredFieldStatus status = new Status.StoredFieldStatus();

    try {
      if (infoStream != null) {
        infoStream.print("    test: stored fields.......");
      }

      // Scan stored fields for all documents
      final Bits liveDocs = reader.getLiveDocs();
      for (int j = 0; j < reader.maxDoc(); ++j) {
        // Intentionally pull even deleted documents to
        // make sure they too are not corrupt:
        Document doc = reader.document(j);
        if (liveDocs == null || liveDocs.get(j)) {
          status.docCount++;
          status.totFields += doc.getFields().size();
        }
      }      

      // Validate docCount
      if (status.docCount != reader.numDocs()) {
        throw new RuntimeException("docCount=" + status.docCount + " but saw " + status.docCount + " undeleted docs");
      }

      msg(infoStream, String.format(Locale.ROOT, "OK [%d total field count; avg %.1f fields per doc] [took %.3f sec]",
                                    status.totFields,
                                    (((float) status.totFields)/status.docCount),
                                    nsToSec(System.nanoTime() - startNS)));
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  /**
   * Test docvalues.
   * @lucene.experimental
   */
  public static Status.DocValuesStatus testDocValues(LeafReader reader,
                                                     PrintStream infoStream,
                                                     boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.DocValuesStatus status = new Status.DocValuesStatus();
    try {
      if (infoStream != null) {
        infoStream.print("    test: docvalues...........");
      }
      for (FieldInfo fieldInfo : reader.getFieldInfos()) {
        if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
          status.totalValueFields++;
          checkDocValues(fieldInfo, reader, infoStream, status);
        } else {
          if (reader.getBinaryDocValues(fieldInfo.name) != null ||
              reader.getNumericDocValues(fieldInfo.name) != null ||
              reader.getSortedDocValues(fieldInfo.name) != null || 
              reader.getSortedSetDocValues(fieldInfo.name) != null || 
              reader.getDocsWithField(fieldInfo.name) != null) {
            throw new RuntimeException("field: " + fieldInfo.name + " has docvalues but should omit them!");
          }
        }
      }

      msg(infoStream, String.format(Locale.ROOT,
                                    "OK [%d docvalues fields; %d BINARY; %d NUMERIC; %d SORTED; %d SORTED_NUMERIC; %d SORTED_SET] [took %.3f sec]",
                                    status.totalValueFields,
                                    status.totalBinaryFields,
                                    status.totalNumericFields,
                                    status.totalSortedFields,
                                    status.totalSortedNumericFields,
                                    status.totalSortedSetFields,
                                    nsToSec(System.nanoTime()-startNS)));
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    return status;
  }
  
  private static void checkBinaryDocValues(String fieldName, LeafReader reader, BinaryDocValues dv, Bits docsWithField) {
    for (int i = 0; i < reader.maxDoc(); i++) {
      final BytesRef term = dv.get(i);
      assert term.isValid();
      if (docsWithField.get(i) == false && term.length > 0) {
        throw new RuntimeException("dv for field: " + fieldName + " is missing but has value=" + term + " for doc: " + i);
      }
    }
  }
  
  private static void checkSortedDocValues(String fieldName, LeafReader reader, SortedDocValues dv, Bits docsWithField) {
    checkBinaryDocValues(fieldName, reader, dv, docsWithField);
    final int maxOrd = dv.getValueCount()-1;
    FixedBitSet seenOrds = new FixedBitSet(dv.getValueCount());
    int maxOrd2 = -1;
    for (int i = 0; i < reader.maxDoc(); i++) {
      int ord = dv.getOrd(i);
      if (ord == -1) {
        if (docsWithField.get(i)) {
          throw new RuntimeException("dv for field: " + fieldName + " has -1 ord but is not marked missing for doc: " + i);
        }
      } else if (ord < -1 || ord > maxOrd) {
        throw new RuntimeException("ord out of bounds: " + ord);
      } else {
        if (!docsWithField.get(i)) {
          throw new RuntimeException("dv for field: " + fieldName + " is missing but has ord=" + ord + " for doc: " + i);
        }
        maxOrd2 = Math.max(maxOrd2, ord);
        seenOrds.set(ord);
      }
    }
    if (maxOrd != maxOrd2) {
      throw new RuntimeException("dv for field: " + fieldName + " reports wrong maxOrd=" + maxOrd + " but this is not the case: " + maxOrd2);
    }
    if (seenOrds.cardinality() != dv.getValueCount()) {
      throw new RuntimeException("dv for field: " + fieldName + " has holes in its ords, valueCount=" + dv.getValueCount() + " but only used: " + seenOrds.cardinality());
    }
    BytesRef lastValue = null;
    for (int i = 0; i <= maxOrd; i++) {
      final BytesRef term = dv.lookupOrd(i);
      assert term.isValid();
      if (lastValue != null) {
        if (term.compareTo(lastValue) <= 0) {
          throw new RuntimeException("dv for field: " + fieldName + " has ords out of order: " + lastValue + " >=" + term);
        }
      }
      lastValue = BytesRef.deepCopyOf(term);
    }
  }
  
  private static void checkSortedSetDocValues(String fieldName, LeafReader reader, SortedSetDocValues dv, Bits docsWithField) {
    final long maxOrd = dv.getValueCount()-1;
    LongBitSet seenOrds = new LongBitSet(dv.getValueCount());
    long maxOrd2 = -1;
    for (int i = 0; i < reader.maxDoc(); i++) {
      dv.setDocument(i);
      long lastOrd = -1;
      long ord;
      if (docsWithField.get(i)) {
        int ordCount = 0;
        while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          if (ord <= lastOrd) {
            throw new RuntimeException("ords out of order: " + ord + " <= " + lastOrd + " for doc: " + i);
          }
          if (ord < 0 || ord > maxOrd) {
            throw new RuntimeException("ord out of bounds: " + ord);
          }
          if (dv instanceof RandomAccessOrds) {
            long ord2 = ((RandomAccessOrds)dv).ordAt(ordCount);
            if (ord != ord2) {
              throw new RuntimeException("ordAt(" + ordCount + ") inconsistent, expected=" + ord + ",got=" + ord2 + " for doc: " + i);
            }
          }
          lastOrd = ord;
          maxOrd2 = Math.max(maxOrd2, ord);
          seenOrds.set(ord);
          ordCount++;
        }
        if (ordCount == 0) {
          throw new RuntimeException("dv for field: " + fieldName + " has no ordinals but is not marked missing for doc: " + i);
        }
        if (dv instanceof RandomAccessOrds) {
          long ordCount2 = ((RandomAccessOrds)dv).cardinality();
          if (ordCount != ordCount2) {
            throw new RuntimeException("cardinality inconsistent, expected=" + ordCount + ",got=" + ordCount2 + " for doc: " + i);
          }
        }
      } else {
        long o = dv.nextOrd();
        if (o != SortedSetDocValues.NO_MORE_ORDS) {
          throw new RuntimeException("dv for field: " + fieldName + " is marked missing but has ord=" + o + " for doc: " + i);
        }
        if (dv instanceof RandomAccessOrds) {
          long ordCount2 = ((RandomAccessOrds)dv).cardinality();
          if (ordCount2 != 0) {
            throw new RuntimeException("dv for field: " + fieldName + " is marked missing but has cardinality " + ordCount2 + " for doc: " + i);
          }
        }
      }
    }
    if (maxOrd != maxOrd2) {
      throw new RuntimeException("dv for field: " + fieldName + " reports wrong maxOrd=" + maxOrd + " but this is not the case: " + maxOrd2);
    }
    if (seenOrds.cardinality() != dv.getValueCount()) {
      throw new RuntimeException("dv for field: " + fieldName + " has holes in its ords, valueCount=" + dv.getValueCount() + " but only used: " + seenOrds.cardinality());
    }
    
    BytesRef lastValue = null;
    for (long i = 0; i <= maxOrd; i++) {
      final BytesRef term = dv.lookupOrd(i);
      assert term.isValid();
      if (lastValue != null) {
        if (term.compareTo(lastValue) <= 0) {
          throw new RuntimeException("dv for field: " + fieldName + " has ords out of order: " + lastValue + " >=" + term);
        }
      }
      lastValue = BytesRef.deepCopyOf(term);
    }
  }
  
  private static void checkSortedNumericDocValues(String fieldName, LeafReader reader, SortedNumericDocValues ndv, Bits docsWithField) {
    for (int i = 0; i < reader.maxDoc(); i++) {
      ndv.setDocument(i);
      int count = ndv.count();
      if (docsWithField.get(i)) {
        if (count == 0) {
          throw new RuntimeException("dv for field: " + fieldName + " is not marked missing but has zero count for doc: " + i);
        }
        long previous = Long.MIN_VALUE;
        for (int j = 0; j < count; j++) {
          long value = ndv.valueAt(j);
          if (value < previous) {
            throw new RuntimeException("values out of order: " + value + " < " + previous + " for doc: " + i);
          }
          previous = value;
        }
      } else {
        if (count != 0) {
          throw new RuntimeException("dv for field: " + fieldName + " is marked missing but has count=" + count + " for doc: " + i);
        }
      }
    }
  }

  private static void checkNumericDocValues(String fieldName, LeafReader reader, NumericDocValues ndv, Bits docsWithField) {
    for (int i = 0; i < reader.maxDoc(); i++) {
      long value = ndv.get(i);
      if (docsWithField.get(i) == false && value != 0) {
        throw new RuntimeException("dv for field: " + fieldName + " is marked missing but has value=" + value + " for doc: " + i);
      }
    }
  }
  
  private static void checkDocValues(FieldInfo fi, LeafReader reader, PrintStream infoStream, DocValuesStatus status) throws Exception {
    Bits docsWithField = reader.getDocsWithField(fi.name);
    if (docsWithField == null) {
      throw new RuntimeException(fi.name + " docsWithField does not exist");
    } else if (docsWithField.length() != reader.maxDoc()) {
      throw new RuntimeException(fi.name + " docsWithField has incorrect length: " + docsWithField.length() + ",expected: " + reader.maxDoc());
    }
    switch(fi.getDocValuesType()) {
      case SORTED:
        status.totalSortedFields++;
        checkSortedDocValues(fi.name, reader, reader.getSortedDocValues(fi.name), docsWithField);
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedNumericDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case SORTED_NUMERIC:
        status.totalSortedNumericFields++;
        checkSortedNumericDocValues(fi.name, reader, reader.getSortedNumericDocValues(fi.name), docsWithField);
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case SORTED_SET:
        status.totalSortedSetFields++;
        checkSortedSetDocValues(fi.name, reader, reader.getSortedSetDocValues(fi.name), docsWithField);
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedNumericDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case BINARY:
        status.totalBinaryFields++;
        checkBinaryDocValues(fi.name, reader, reader.getBinaryDocValues(fi.name), docsWithField);
        if (reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null ||
            reader.getSortedNumericDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case NUMERIC:
        status.totalNumericFields++;
        checkNumericDocValues(fi.name, reader, reader.getNumericDocValues(fi.name), docsWithField);
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null ||
            reader.getSortedNumericDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      default:
        throw new AssertionError();
    }
  }
  
  private static void checkNorms(FieldInfo fi, LeafReader reader, PrintStream infoStream) throws IOException {
    if (fi.hasNorms()) {
      checkNumericDocValues(fi.name, reader, reader.getNormValues(fi.name), new Bits.MatchAllBits(reader.maxDoc()));
    }
  }

  /**
   * Test term vectors.
   * @lucene.experimental
   */
  public static Status.TermVectorStatus testTermVectors(LeafReader reader, PrintStream infoStream) throws IOException {
    return testTermVectors(reader, infoStream, false, false, false);
  }

  /**
   * Test term vectors.
   * @lucene.experimental
   */
  public static Status.TermVectorStatus testTermVectors(LeafReader reader, PrintStream infoStream, boolean verbose, boolean crossCheckTermVectors, boolean failFast) throws IOException {
    long startNS = System.nanoTime();
    final Status.TermVectorStatus status = new Status.TermVectorStatus();
    final FieldInfos fieldInfos = reader.getFieldInfos();
    final Bits onlyDocIsDeleted = new FixedBitSet(1);
    
    try {
      if (infoStream != null) {
        infoStream.print("    test: term vectors........");
      }

      PostingsEnum docs = null;
      PostingsEnum postings = null;

      // Only used if crossCheckTermVectors is true:
      PostingsEnum postingsDocs = null;
      PostingsEnum postingsPostings = null;

      final Bits liveDocs = reader.getLiveDocs();

      final Fields postingsFields;
      // TODO: testTermsIndex
      if (crossCheckTermVectors) {
        postingsFields = reader.fields();
      } else {
        postingsFields = null;
      }

      TermsEnum termsEnum = null;
      TermsEnum postingsTermsEnum = null;

      for (int j = 0; j < reader.maxDoc(); ++j) {
        // Intentionally pull/visit (but don't count in
        // stats) deleted documents to make sure they too
        // are not corrupt:
        Fields tfv = reader.getTermVectors(j);

        // TODO: can we make a IS(FIR) that searches just
        // this term vector... to pass for searcher?

        if (tfv != null) {
          // First run with no deletions:
          checkFields(tfv, null, 1, fieldInfos, false, true, infoStream, verbose);

          if (j == 0) {
            // Also test with the 1 doc deleted; we only do this for first doc because this really is just looking for a [slightly] buggy
            // TermVectors impl that fails to respect the incoming live docs:
            checkFields(tfv, onlyDocIsDeleted, 1, fieldInfos, false, true, infoStream, verbose);
          }

          // Only agg stats if the doc is live:
          final boolean doStats = liveDocs == null || liveDocs.get(j);

          if (doStats) {
            status.docCount++;
          }

          for(String field : tfv) {
            if (doStats) {
              status.totVectors++;
            }

            // Make sure FieldInfo thinks this field is vector'd:
            final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
            if (!fieldInfo.hasVectors()) {
              throw new RuntimeException("docID=" + j + " has term vectors for field=" + field + " but FieldInfo has storeTermVector=false");
            }

            if (crossCheckTermVectors) {
              Terms terms = tfv.terms(field);
              termsEnum = terms.iterator(termsEnum);
              final boolean postingsHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
              final boolean postingsHasPayload = fieldInfo.hasPayloads();
              final boolean vectorsHasPayload = terms.hasPayloads();

              Terms postingsTerms = postingsFields.terms(field);
              if (postingsTerms == null) {
                throw new RuntimeException("vector field=" + field + " does not exist in postings; doc=" + j);
              }
              postingsTermsEnum = postingsTerms.iterator(postingsTermsEnum);
              
              final boolean hasProx = terms.hasOffsets() || terms.hasPositions();
              BytesRef term = null;
              while ((term = termsEnum.next()) != null) {

                if (hasProx) {
                  postings = termsEnum.postings(null, postings, PostingsEnum.ALL);
                  assert postings != null;
                  docs = null;
                } else {
                  docs = termsEnum.postings(null, docs);
                  assert docs != null;
                  postings = null;
                }

                final PostingsEnum docs2;
                if (hasProx) {
                  assert postings != null;
                  docs2 = postings;
                } else {
                  assert docs != null;
                  docs2 = docs;
                }

                final PostingsEnum postingsDocs2;
                if (!postingsTermsEnum.seekExact(term)) {
                  throw new RuntimeException("vector term=" + term + " field=" + field + " does not exist in postings; doc=" + j);
                }
                postingsPostings = postingsTermsEnum.postings(null, postingsPostings, PostingsEnum.ALL);
                if (postingsPostings == null) {
                  // Term vectors were indexed w/ pos but postings were not
                  postingsDocs = postingsTermsEnum.postings(null, postingsDocs);
                  if (postingsDocs == null) {
                    throw new RuntimeException("vector term=" + term + " field=" + field + " does not exist in postings; doc=" + j);
                  }
                }

                if (postingsPostings != null) {
                  postingsDocs2 = postingsPostings;
                } else {
                  postingsDocs2 = postingsDocs;
                }
                  
                final int advanceDoc = postingsDocs2.advance(j);
                if (advanceDoc != j) {
                  throw new RuntimeException("vector term=" + term + " field=" + field + ": doc=" + j + " was not found in postings (got: " + advanceDoc + ")");
                }

                final int doc = docs2.nextDoc();
                  
                if (doc != 0) {
                  throw new RuntimeException("vector for doc " + j + " didn't return docID=0: got docID=" + doc);
                }

                if (postingsHasFreq) {
                  final int tf = docs2.freq();
                  if (postingsHasFreq && postingsDocs2.freq() != tf) {
                    throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": freq=" + tf + " differs from postings freq=" + postingsDocs2.freq());
                  }
                
                  if (hasProx) {
                    for (int i = 0; i < tf; i++) {
                      int pos = postings.nextPosition();
                      if (postingsPostings != null) {
                        int postingsPos = postingsPostings.nextPosition();
                        if (terms.hasPositions() && pos != postingsPos) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": pos=" + pos + " differs from postings pos=" + postingsPos);
                        }
                      }

                      // Call the methods to at least make
                      // sure they don't throw exc:
                      final int startOffset = postings.startOffset();
                      final int endOffset = postings.endOffset();
                      // TODO: these are too anal...?
                      /*
                        if (endOffset < startOffset) {
                        throw new RuntimeException("vector startOffset=" + startOffset + " is > endOffset=" + endOffset);
                        }
                        if (startOffset < lastStartOffset) {
                        throw new RuntimeException("vector startOffset=" + startOffset + " is < prior startOffset=" + lastStartOffset);
                        }
                        lastStartOffset = startOffset;
                      */

                      if (postingsPostings != null) {
                        final int postingsStartOffset = postingsPostings.startOffset();

                        final int postingsEndOffset = postingsPostings.endOffset();
                        if (startOffset != -1 && postingsStartOffset != -1 && startOffset != postingsStartOffset) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": startOffset=" + startOffset + " differs from postings startOffset=" + postingsStartOffset);
                        }
                        if (endOffset != -1 && postingsEndOffset != -1 && endOffset != postingsEndOffset) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": endOffset=" + endOffset + " differs from postings endOffset=" + postingsEndOffset);
                        }
                      }
                      
                      BytesRef payload = postings.getPayload();
           
                      if (payload != null) {
                        assert vectorsHasPayload;
                      }
                      
                      if (postingsHasPayload && vectorsHasPayload) {
                        assert postingsPostings != null;
                        
                        if (payload == null) {
                          // we have payloads, but not at this position. 
                          // postings has payloads too, it should not have one at this position
                          if (postingsPostings.getPayload() != null) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has no payload but postings does: " + postingsPostings.getPayload());
                          }
                        } else {
                          // we have payloads, and one at this position
                          // postings should also have one at this position, with the same bytes.
                          if (postingsPostings.getPayload() == null) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has payload=" + payload + " but postings does not.");
                          }
                          BytesRef postingsPayload = postingsPostings.getPayload();
                          if (!payload.equals(postingsPayload)) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has payload=" + payload + " but differs from postings payload=" + postingsPayload);
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      float vectorAvg = status.docCount == 0 ? 0 : status.totVectors / (float)status.docCount;
      msg(infoStream, String.format(Locale.ROOT, "OK [%d total term vector count; avg %.1f term/freq vector fields per doc] [took %.3f sec]",
                                    status.totVectors, vectorAvg, nsToSec(System.nanoTime() - startNS)));
    } catch (Throwable e) {
      if (failFast) {
        IOUtils.reThrow(e);
      }
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    
    return status;
  }

  /** Repairs the index using previously returned result
   *  from {@link #checkIndex}.  Note that this does not
   *  remove any of the unreferenced files after it's done;
   *  you must separately open an {@link IndexWriter}, which
   *  deletes unreferenced files when it's created.
   *
   * <p><b>WARNING</b>: this writes a
   *  new segments file into the index, effectively removing
   *  all documents in broken segments from the index.
   *  BE CAREFUL.
   */
  public void exorciseIndex(Status result) throws IOException {
    ensureOpen();
    if (result.partial)
      throw new IllegalArgumentException("can only exorcise an index that was fully checked (this status checked a subset of segments)");
    result.newSegments.changed();
    result.newSegments.commit(result.dir);
  }

  private static boolean assertsOn;

  private static boolean testAsserts() {
    assertsOn = true;
    return true;
  }

  private static boolean assertsOn() {
    assert testAsserts();
    return assertsOn;
  }

  /** Command-line interface to check and exorcise corrupt segments from an index.

    <p>
    Run it like this:
    <pre>
    java -ea:org.apache.lucene... org.apache.lucene.index.CheckIndex pathToIndex [-exorcise] [-verbose] [-segment X] [-segment Y]
    </pre>
    <ul>
    <li><code>-exorcise</code>: actually write a new segments_N file, removing any problematic segments. *LOSES DATA*

    <li><code>-segment X</code>: only check the specified
    segment(s).  This can be specified multiple times,
    to check more than one segment, eg <code>-segment _2
    -segment _a</code>.  You can't use this with the -exorcise
    option.
    </ul>

    <p><b>WARNING</b>: <code>-exorcise</code> should only be used on an emergency basis as it will cause
                       documents (perhaps many) to be permanently removed from the index.  Always make
                       a backup copy of your index before running this!  Do not run this tool on an index
                       that is actively being written to.  You have been warned!

    <p>                Run without -exorcise, this tool will open the index, report version information
                       and report any exceptions it hits and what action it would take if -exorcise were
                       specified.  With -exorcise, this tool will remove any segments that have issues and
                       write a new segments_N file.  This means all documents contained in the affected
                       segments will be removed.

    <p>
                       This tool exits with exit code 1 if the index cannot be opened or has any
                       corruption, else 0.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    int exitCode = doMain(args);
    System.exit(exitCode);
  }
  
  // actual main: returns exit code instead of terminating JVM (for easy testing)
  private static int doMain(String args[]) throws IOException, InterruptedException {

    boolean doExorcise = false;
    boolean doCrossCheckTermVectors = false;
    boolean verbose = false;
    List<String> onlySegments = new ArrayList<>();
    String indexPath = null;
    String dirImpl = null;
    int i = 0;
    while(i < args.length) {
      String arg = args[i];
      if ("-exorcise".equals(arg)) {
        doExorcise = true;
      } else if ("-crossCheckTermVectors".equals(arg)) {
        doCrossCheckTermVectors = true;
      } else if (arg.equals("-verbose")) {
        verbose = true;
      } else if (arg.equals("-segment")) {
        if (i == args.length-1) {
          System.out.println("ERROR: missing name for -segment option");
          return 1;
        }
        i++;
        onlySegments.add(args[i]);
      } else if ("-dir-impl".equals(arg)) {
        if (i == args.length - 1) {
          System.out.println("ERROR: missing value for -dir-impl option");
          return 1;
        }
        i++;
        dirImpl = args[i];
      } else {
        if (indexPath != null) {
          System.out.println("ERROR: unexpected extra argument '" + args[i] + "'");
          return 1;
        }
        indexPath = args[i];
      }
      i++;
    }

    if (indexPath == null) {
      System.out.println("\nERROR: index path not specified");
      System.out.println("\nUsage: java org.apache.lucene.index.CheckIndex pathToIndex [-exorcise] [-crossCheckTermVectors] [-segment X] [-segment Y] [-dir-impl X]\n" +
                         "\n" +
                         "  -exorcise: actually write a new segments_N file, removing any problematic segments\n" +
                         "  -crossCheckTermVectors: verifies that term vectors match postings; THIS IS VERY SLOW!\n" +
                         "  -codec X: when exorcising, codec to write the new segments_N file with\n" +
                         "  -verbose: print additional details\n" +
                         "  -segment X: only check the specified segments.  This can be specified multiple\n" + 
                         "              times, to check more than one segment, eg '-segment _2 -segment _a'.\n" +
                         "              You can't use this with the -exorcise option\n" +
                         "  -dir-impl X: use a specific " + FSDirectory.class.getSimpleName() + " implementation. " +
                         "If no package is specified the " + FSDirectory.class.getPackage().getName() + " package will be used.\n" +
                         "\n" +
                         "**WARNING**: -exorcise *LOSES DATA*. This should only be used on an emergency basis as it will cause\n" +
                         "documents (perhaps many) to be permanently removed from the index.  Always make\n" +
                         "a backup copy of your index before running this!  Do not run this tool on an index\n" +
                         "that is actively being written to.  You have been warned!\n" +
                         "\n" +
                         "Run without -exorcise, this tool will open the index, report version information\n" +
                         "and report any exceptions it hits and what action it would take if -exorcise were\n" +
                         "specified.  With -exorcise, this tool will remove any segments that have issues and\n" + 
                         "write a new segments_N file.  This means all documents contained in the affected\n" +
                         "segments will be removed.\n" +
                         "\n" +
                         "This tool exits with exit code 1 if the index cannot be opened or has any\n" +
                         "corruption, else 0.\n");
      return 1;
    }

    if (!assertsOn())
      System.out.println("\nNOTE: testing will be more thorough if you run java with '-ea:org.apache.lucene...', so assertions are enabled");

    if (onlySegments.size() == 0)
      onlySegments = null;
    else if (doExorcise) {
      System.out.println("ERROR: cannot specify both -exorcise and -segment");
      return 1;
    }

    System.out.println("\nOpening index @ " + indexPath + "\n");
    Directory directory = null;
    Path path = Paths.get(indexPath);
    try {
      if (dirImpl == null) {
        directory = FSDirectory.open(path);
      } else {
        directory = CommandLineUtil.newFSDirectory(dirImpl, path);
      }
    } catch (Throwable t) {
      System.out.println("ERROR: could not open directory \"" + indexPath + "\"; exiting");
      t.printStackTrace(System.out);
      return 1;
    }

    try (Directory dir = directory;
         CheckIndex checker = new CheckIndex(dir)) {
      checker.setCrossCheckTermVectors(doCrossCheckTermVectors);
      checker.setInfoStream(System.out, verbose);
      
      Status result = checker.checkIndex(onlySegments);
      if (result.missingSegments) {
        return 1;
      }
      
      if (!result.clean) {
        if (!doExorcise) {
          System.out.println("WARNING: would write new segments file, and " + result.totLoseDocCount + " documents would be lost, if -exorcise were specified\n");
        } else {
          System.out.println("WARNING: " + result.totLoseDocCount + " documents will be lost\n");
          System.out.println("NOTE: will write new segments file in 5 seconds; this will remove " + result.totLoseDocCount + " docs from the index. YOU WILL LOSE DATA. THIS IS YOUR LAST CHANCE TO CTRL+C!");
          for(int s=0;s<5;s++) {
            Thread.sleep(1000);
            System.out.println("  " + (5-s) + "...");
          }
          System.out.println("Writing...");
          checker.exorciseIndex(result);
          System.out.println("OK");
          System.out.println("Wrote new segments file \"" + result.newSegments.getSegmentsFileName() + "\"");
        }
      }
      System.out.println("");
      
      if (result.clean == true) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  private static double nsToSec(long ns) {
    return ns/1000000000.0;
  }
}
