package org.apache.lucene.codecs.memory;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.CodecUtil;

/**
 * FST-based terms dictionary reader.
 *
 * The FST directly maps each term and its metadata, 
 * it is memeory resident.
 *
 * @lucene.experimental
 */

public class FSTTermsReader extends FieldsProducer {
  final TreeMap<String, TermsReader> fields = new TreeMap<String, TermsReader>();
  final PostingsReaderBase postingsReader;
  final IndexInput in;
  //static boolean TEST = false;

  public FSTTermsReader(SegmentReadState state, PostingsReaderBase postingsReader) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, FSTTermsWriter.TERMS_EXTENSION);

    this.postingsReader = postingsReader;
    this.in = state.directory.openInput(termsFileName, state.context);

    boolean success = false;
    try {
      readHeader(in);
      this.postingsReader.init(in);
      seekDir(in);

      final FieldInfos fieldInfos = state.fieldInfos;
      final int numFields = in.readVInt();
      for (int i = 0; i < numFields; i++) {
        int fieldNumber = in.readVInt();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
        long numTerms = in.readVLong();
        long sumTotalTermFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : in.readVLong();
        long sumDocFreq = in.readVLong();
        int docCount = in.readVInt();
        int longsSize = in.readVInt();
        TermsReader current = new TermsReader(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize);
        TermsReader previous = fields.put(fieldInfo.name, current);
        checkFieldSummary(state.segmentInfo, current, previous);
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private int readHeader(IndexInput in) throws IOException {
    return CodecUtil.checkHeader(in, FSTTermsWriter.TERMS_CODEC_NAME,
                                     FSTTermsWriter.TERMS_VERSION_START,
                                     FSTTermsWriter.TERMS_VERSION_CURRENT);
  }
  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, TermsReader field, TermsReader previous) throws IOException {
    // #docs with field must be <= #docs
    if (field.docCount < 0 || field.docCount > info.getDocCount()) {
      throw new CorruptIndexException("invalid docCount: " + field.docCount + " maxDoc: " + info.getDocCount() + " (resource=" + in + ")");
    }
    // #postings must be >= #docs with field
    if (field.sumDocFreq < field.docCount) {
      throw new CorruptIndexException("invalid sumDocFreq: " + field.sumDocFreq + " docCount: " + field.docCount + " (resource=" + in + ")");
    }
    // #positions must be >= #postings
    if (field.sumTotalTermFreq != -1 && field.sumTotalTermFreq < field.sumDocFreq) {
      throw new CorruptIndexException("invalid sumTotalTermFreq: " + field.sumTotalTermFreq + " sumDocFreq: " + field.sumDocFreq + " (resource=" + in + ")");
    }
    if (previous != null) {
      throw new CorruptIndexException("duplicate fields: " + field.fieldInfo.name + " (resource=" + in + ")");
    }
  }

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    assert field != null;
    return fields.get(field);
  }

  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(in, postingsReader);
    } finally {
      fields.clear();
    }
  }

  final class TermsReader extends Terms {
    final FieldInfo fieldInfo;
    final long numTerms;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;
    final int longsSize;
    final FST<FSTTermOutputs.TermData> dict;

    TermsReader(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize) throws IOException {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.dict = new FST<FSTTermOutputs.TermData>(in, new FSTTermOutputs(fieldInfo, longsSize));
    }

    @Override
    public boolean hasOffsets() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return fieldInfo.hasPayloads();
    }

    @Override
    public long size() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new IntersectTermsEnum(compiled, startTerm);
    }

    // Only wraps common operations for PBF interact
    abstract class BaseTermsEnum extends TermsEnum {
      /* Current term, null when enum ends or unpositioned */
      BytesRef term;

      /* Current term stats + decoded metadata (customized by PBF) */
      final BlockTermState state;

      /* Current term stats + undecoded metadata (long[] & byte[]) */
      FSTTermOutputs.TermData meta;
      ByteArrayDataInput bytesReader;

      /** Decodes metadata into customized term state */
      abstract void decodeMetaData() throws IOException;

      BaseTermsEnum() throws IOException {
        this.state = postingsReader.newTermState();
        this.bytesReader = new ByteArrayDataInput();
        this.term = null;
        // NOTE: metadata will only be initialized in child class
      }

      @Override
      public TermState termState() throws IOException {
        decodeMetaData();
        return state.clone();
      }

      @Override
      public BytesRef term() {
        return term;
      }

      @Override
      public int docFreq() throws IOException {
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        return state.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        decodeMetaData();
        return postingsReader.docs(fieldInfo, state, liveDocs, reuse, flags);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        if (!hasPositions()) {
          return null;
        }
        decodeMetaData();
        return postingsReader.docsAndPositions(fieldInfo, state, liveDocs, reuse, flags);
      }

      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }
    }


    // Iterates through all terms in this field
    private final class SegmentTermsEnum extends BaseTermsEnum {
      final BytesRefFSTEnum<FSTTermOutputs.TermData> fstEnum;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when current enum is 'positioned' by seekExact(TermState) */
      boolean seekPending;

      SegmentTermsEnum() throws IOException {
        super();
        this.fstEnum = new BytesRefFSTEnum<FSTTermOutputs.TermData>(dict);
        this.decoded = false;
        this.seekPending = false;
        this.meta = null;
      }

      // Let PBF decode metadata from long[] and byte[]
      @Override
      void decodeMetaData() throws IOException {
        if (!decoded && !seekPending) {
          if (meta.bytes != null) {
            bytesReader.reset(meta.bytes, 0, meta.bytes.length);
          }
          postingsReader.decodeTerm(meta.longs, bytesReader, fieldInfo, state, true);
          decoded = true;
        }
      }

      // Update current enum according to FSTEnum
      void updateEnum(final InputOutput<FSTTermOutputs.TermData> pair) {
        if (pair == null) {
          term = null;
        } else {
          term = pair.input;
          meta = pair.output;
          state.docFreq = meta.docFreq;
          state.totalTermFreq = meta.totalTermFreq;
        }
        decoded = false;
        seekPending = false;
      }

      @Override
      public BytesRef next() throws IOException {
        if (seekPending) {  // previously positioned, but termOutputs not fetched
          seekPending = false;
          SeekStatus status = seekCeil(term);
          assert status == SeekStatus.FOUND;  // must positioned on valid term
        }
        updateEnum(fstEnum.next());
        return term;
      }

      @Override
      public boolean seekExact(BytesRef target) throws IOException {
        updateEnum(fstEnum.seekExact(target));
        return term != null;
      }

      @Override
      public SeekStatus seekCeil(BytesRef target) throws IOException {
        updateEnum(fstEnum.seekCeil(target));
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) {
        if (!target.equals(term)) {
          state.copyFrom(otherState);
          term = BytesRef.deepCopyOf(target);
          seekPending = true;
        }
      }
    }

    // Iterates intersect result with automaton (cannot seek!)
    private final class IntersectTermsEnum extends BaseTermsEnum {
      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when there is pending term when calling next() */
      boolean pending;

      /* stack to record how current term is constructed, 
       * used to accumulate metadata or rewind term:
       *   level == term.length + 1,
       *         == 0 when term is null */
      Frame[] stack;
      int level;

      /* to which level the metadata is accumulated 
       * so that we can accumulate metadata lazily */
      int metaUpto;

      /* term dict fst */
      final FST<FSTTermOutputs.TermData> fst;
      final FST.BytesReader fstReader;
      final Outputs<FSTTermOutputs.TermData> fstOutputs;

      /* query automaton to intersect with */
      final ByteRunAutomaton fsa;

      private final class Frame {
        /* fst stats */
        FST.Arc<FSTTermOutputs.TermData> fstArc;

        /* automaton stats */
        int fsaState;

        Frame() {
          this.fstArc = new FST.Arc<FSTTermOutputs.TermData>();
          this.fsaState = -1;
        }

        public String toString() {
          return "arc=" + fstArc + " state=" + fsaState;
        }
      }

      IntersectTermsEnum(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        super();
        //if (TEST) System.out.println("Enum init, startTerm=" + startTerm);
        this.fst = dict;
        this.fstReader = fst.getBytesReader();
        this.fstOutputs = dict.outputs;
        this.fsa = compiled.runAutomaton;
        this.level = -1;
        this.stack = new Frame[16];
        for (int i = 0 ; i < stack.length; i++) {
          this.stack[i] = new Frame();
        }

        Frame frame;
        frame = loadVirtualFrame(newFrame());
        this.level++;
        frame = loadFirstFrame(newFrame());
        pushFrame(frame);

        this.meta = null;
        this.metaUpto = 1;
        this.decoded = false;
        this.pending = false;

        if (startTerm == null) {
          pending = isAccept(topFrame());
        } else {
          doSeekCeil(startTerm);
          pending = !startTerm.equals(term) && isValid(topFrame()) && isAccept(topFrame());
        }
      }

      @Override
      void decodeMetaData() throws IOException {
        assert term != null;
        if (!decoded) {
          if (meta.bytes != null) {
            bytesReader.reset(meta.bytes, 0, meta.bytes.length);
          }
          postingsReader.decodeTerm(meta.longs, bytesReader, fieldInfo, state, true);
          decoded = true;
        }
      }

      /** Lazily accumulate meta data, when we got a accepted term */
      void loadMetaData() throws IOException {
        FST.Arc<FSTTermOutputs.TermData> last, next;
        last = stack[metaUpto].fstArc;
        while (metaUpto != level) {
          metaUpto++;
          next = stack[metaUpto].fstArc;
          next.output = fstOutputs.add(next.output, last.output);
          last = next;
        }
        if (last.isFinal()) {
          meta = fstOutputs.add(last.output, last.nextFinalOutput);
        } else {
          meta = last.output;
        }
        state.docFreq = meta.docFreq;
        state.totalTermFreq = meta.totalTermFreq;
      }

      @Override
      public SeekStatus seekCeil(BytesRef target) throws IOException {
        decoded = false;
        term = doSeekCeil(target);
        loadMetaData();
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public BytesRef next() throws IOException {
        //if (TEST) System.out.println("Enum next()");
        if (pending) {
          pending = false;
          loadMetaData();
          return term;
        }
        decoded = false;
      DFS:
        while (level > 0) {
          Frame frame = newFrame();
          if (loadExpandFrame(topFrame(), frame) != null) {  // has valid target
            pushFrame(frame);
            if (isAccept(frame)) {  // gotcha
              break;
            }
            continue;  // check next target
          } 
          frame = popFrame();
          while(level > 0) {
            if (loadNextFrame(topFrame(), frame) != null) {  // has valid sibling 
              pushFrame(frame);
              if (isAccept(frame)) {  // gotcha
                break DFS;
              }
              continue DFS;   // check next target 
            }
            frame = popFrame();
          }
          return null;
        }
        loadMetaData();
        return term;
      }

      private BytesRef doSeekCeil(BytesRef target) throws IOException {
        //if (TEST) System.out.println("Enum doSeekCeil()");
        Frame frame= null;
        int label, upto = 0, limit = target.length;
        while (upto < limit) {  // to target prefix, or ceil label (rewind prefix)
          frame = newFrame();
          label = target.bytes[upto] & 0xff;
          frame = loadCeilFrame(label, topFrame(), frame);
          if (frame == null || frame.fstArc.label != label) {
            break;
          }
          assert isValid(frame);  // target must be fetched from automaton
          pushFrame(frame);
          upto++;
        }
        if (upto == limit) {  // got target
          return term;
        }
        if (frame != null) {  // got larger term('s prefix)
          pushFrame(frame);
          return isAccept(frame) ? term : next();
        }
        while (level > 0) {  // got target's prefix, advance to larger term
          frame = popFrame();
          while (level > 0 && !canRewind(frame)) {
            frame = popFrame();
          }
          if (loadNextFrame(topFrame(), frame) != null) {
            pushFrame(frame);
            return isAccept(frame) ? term : next();
          }
        }
        return null;
      }

      /** Virtual frame, never pop */
      Frame loadVirtualFrame(Frame frame) throws IOException {
        frame.fstArc.output = fstOutputs.getNoOutput();
        frame.fstArc.nextFinalOutput = fstOutputs.getNoOutput();
        frame.fsaState = -1;
        return frame;
      }

      /** Load frame for start arc(node) on fst */
      Frame loadFirstFrame(Frame frame) throws IOException {
        frame.fstArc = fst.getFirstArc(frame.fstArc);
        frame.fsaState = fsa.getInitialState();
        return frame;
      }

      /** Load frame for target arc(node) on fst */
      Frame loadExpandFrame(Frame top, Frame frame) throws IOException {
        if (!canGrow(top)) {
          return null;
        }
        frame.fstArc = fst.readFirstRealTargetArc(top.fstArc.target, frame.fstArc, fstReader);
        frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label);
        //if (TEST) System.out.println(" loadExpand frame="+frame);
        if (frame.fsaState == -1) {
          return loadNextFrame(top, frame);
        }
        return frame;
      }

      /** Load frame for sibling arc(node) on fst */
      Frame loadNextFrame(Frame top, Frame frame) throws IOException {
        if (!canRewind(frame)) {
          return null;
        }
        while (!frame.fstArc.isLast()) {
          frame.fstArc = fst.readNextRealArc(frame.fstArc, fstReader);
          frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label);
          if (frame.fsaState != -1) {
            break;
          }
        }
        //if (TEST) System.out.println(" loadNext frame="+frame);
        if (frame.fsaState == -1) {
          return null;
        }
        return frame;
      }

      /** Load frame for target arc(node) on fst, so that 
       *  arc.label >= label and !fsa.reject(arc.label) */
      Frame loadCeilFrame(int label, Frame top, Frame frame) throws IOException {
        FST.Arc<FSTTermOutputs.TermData> arc = frame.fstArc;
        arc = Util.readCeilArc(label, fst, top.fstArc, arc, fstReader);
        if (arc == null) {
          return null;
        }
        frame.fsaState = fsa.step(top.fsaState, arc.label);
        //if (TEST) System.out.println(" loadCeil frame="+frame);
        if (frame.fsaState == -1) {
          return loadNextFrame(top, frame);
        }
        return frame;
      }

      boolean isAccept(Frame frame) {  // reach a term both fst&fsa accepts
        return fsa.isAccept(frame.fsaState) && frame.fstArc.isFinal();
      }
      boolean isValid(Frame frame) {   // reach a prefix both fst&fsa won't reject
        return /*frame != null &&*/ frame.fsaState != -1;
      }
      boolean canGrow(Frame frame) {   // can walk forward on both fst&fsa
        return frame.fsaState != -1 && FST.targetHasArcs(frame.fstArc);
      }
      boolean canRewind(Frame frame) { // can jump to sibling
        return !frame.fstArc.isLast();
      }

      void pushFrame(Frame frame) {
        term = grow(frame.fstArc.label);
        level++;
        //if (TEST) System.out.println("  term=" + term + " level=" + level);
      }

      Frame popFrame() {
        term = shrink();
        level--;
        metaUpto = metaUpto > level ? level : metaUpto;
        //if (TEST) System.out.println("  term=" + term + " level=" + level);
        return stack[level+1];
      }

      Frame newFrame() {
        if (level+1 == stack.length) {
          final Frame[] temp = new Frame[ArrayUtil.oversize(level+2, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(stack, 0, temp, 0, stack.length);
          for (int i = stack.length; i < temp.length; i++) {
            temp[i] = new Frame();
          }
          stack = temp;
        }
        return stack[level+1];
      }

      Frame topFrame() {
        return stack[level];
      }

      BytesRef grow(int label) {
        if (term == null) {
          term = new BytesRef(new byte[16], 0, 0);
        } else {
          if (term.length == term.bytes.length) {
            term.grow(term.length+1);
          }
          term.bytes[term.length++] = (byte)label;
        }
        return term;
      }

      BytesRef shrink() {
        if (term.length == 0) {
          term = null;
        } else {
          term.length--;
        }
        return term;
      }
    }
  }

  static<T> void walk(FST<T> fst) throws IOException {
    final ArrayList<FST.Arc<T>> queue = new ArrayList<FST.Arc<T>>();
    final BitSet seen = new BitSet();
    final FST.BytesReader reader = fst.getBytesReader();
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());
    queue.add(startArc);
    while (!queue.isEmpty()) {
      final FST.Arc<T> arc = queue.remove(0);
      final long node = arc.target;
      //System.out.println(arc);
      if (FST.targetHasArcs(arc) && !seen.get((int) node)) {
        seen.set((int) node);
        fst.readFirstRealTargetArc(node, arc, reader);
        while (true) {
          queue.add(new FST.Arc<T>().copyFrom(arc));
          if (arc.isLast()) {
            break;
          } else {
            fst.readNextRealArc(arc, reader);
          }
        }
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = 0;
    for (TermsReader r : fields.values()) {
      ramBytesUsed += r.dict == null ? 0 : r.dict.sizeInBytes();
    }
    return ramBytesUsed;
  }
}
