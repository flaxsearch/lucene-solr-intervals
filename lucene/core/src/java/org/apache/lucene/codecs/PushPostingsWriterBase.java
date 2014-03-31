package org.apache.lucene.codecs;

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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Extension of {@link PostingsWriterBase}, adding a push
 * API for writing each element of the postings.  This API
 * is somewhat analagous to an XML SAX API, while {@link
 * PostingsWriterBase} is more like an XML DOM API.
 * 
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PushPostingsWriterBase extends PostingsWriterBase {

  // Reused in writeTerm
  private DocsEnum docsEnum;
  private DocsAndPositionsEnum posEnum;
  private int enumFlags;

  /** {@link FieldInfo} of current field being written. */
  protected FieldInfo fieldInfo;

  /** {@link IndexOptions} of current field being
      written */
  protected IndexOptions indexOptions;

  /** True if the current field writes freqs. */
  protected boolean writeFreqs;

  /** True if the current field writes positions. */
  protected boolean writePositions;

  /** True if the current field writes payloads. */
  protected boolean writePayloads;

  /** True if the current field writes offsets. */
  protected boolean writeOffsets;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PushPostingsWriterBase() {
  }

  /** Called once after startup, before any terms have been
   *  added.  Implementations typically write a header to
   *  the provided {@code termsOut}. */
  public abstract void init(IndexOutput termsOut) throws IOException;

  /** Return a newly created empty TermState */
  public abstract BlockTermState newTermState() throws IOException;

  /** Start a new term.  Note that a matching call to {@link
   *  #finishTerm(BlockTermState)} is done, only if the term has at least one
   *  document. */
  public abstract void startTerm() throws IOException;

  /** Finishes the current term.  The provided {@link
   *  BlockTermState} contains the term's summary statistics, 
   *  and will holds metadata from PBF when returned */
  public abstract void finishTerm(BlockTermState state) throws IOException;

  /**
   * Encode metadata as long[] and byte[]. {@code absolute} controls whether 
   * current term is delta encoded according to latest term. 
   * Usually elements in {@code longs} are file pointers, so each one always 
   * increases when a new term is consumed. {@code out} is used to write generic
   * bytes, which are not monotonic.
   *
   * NOTE: sometimes long[] might contain "don't care" values that are unused, e.g. 
   * the pointer to postings list may not be defined for some terms but is defined
   * for others, if it is designed to inline  some postings data in term dictionary.
   * In this case, the postings writer should always use the last value, so that each
   * element in metadata long[] remains monotonic.
   */
  public abstract void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException;

  /** 
   * Sets the current field for writing, and returns the
   * fixed length of long[] metadata (which is fixed per
   * field), called when the writing switches to another field. */
  // TODO: better name?
  public int setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.getIndexOptions();

    writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;        
    writePayloads = fieldInfo.hasPayloads();

    if (writeFreqs == false) {
      enumFlags = 0;
    } else if (writePositions == false) {
      enumFlags = DocsEnum.FLAG_FREQS;
    } else if (writeOffsets == false) {
      if (writePayloads) {
        enumFlags = DocsAndPositionsEnum.FLAG_PAYLOADS;
      } else {
        enumFlags = 0;
      }
    } else {
      if (writePayloads) {
        enumFlags = DocsAndPositionsEnum.FLAG_PAYLOADS | DocsAndPositionsEnum.FLAG_OFFSETS;
      } else {
        enumFlags = DocsAndPositionsEnum.FLAG_OFFSETS;
      }
    }

    return 0;
  }

  @Override
  public final BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen) throws IOException {
    startTerm();
    if (writePositions == false) {
      docsEnum = termsEnum.docs(null, docsEnum, enumFlags);
    } else {
      posEnum = termsEnum.docsAndPositions(null, posEnum, enumFlags);
      docsEnum = posEnum;
    }
    assert docsEnum != null;

    int docFreq = 0;
    long totalTermFreq = 0;
    while (true) {
      int docID = docsEnum.nextDoc();
      if (docID == DocsEnum.NO_MORE_DOCS) {
        break;
      }
      docFreq++;
      docsSeen.set(docID);
      int freq;
      if (writeFreqs) {
        freq = docsEnum.freq();
        totalTermFreq += freq;
      } else {
        freq = -1;
      }
      startDoc(docID, freq);

      if (writePositions) {
        for(int i=0;i<freq;i++) {
          int pos = posEnum.nextPosition();
          BytesRef payload = writePayloads ? posEnum.getPayload() : null;
          int startOffset;
          int endOffset;
          if (writeOffsets) {
            startOffset = posEnum.startOffset();
            endOffset = posEnum.endOffset();
          } else {
            startOffset = -1;
            endOffset = -1;
          }
          addPosition(pos, payload, startOffset, endOffset);
        }
      }

      finishDoc();
    }

    if (docFreq == 0) {
      return null;
    } else {
      BlockTermState state = newTermState();
      state.docFreq = docFreq;
      state.totalTermFreq = writeFreqs ? totalTermFreq : -1;
      finishTerm(state);
      return state;
    }
  }

  /** Adds a new doc in this term. 
   * <code>freq</code> will be -1 when term frequencies are omitted
   * for the field. */
  public abstract void startDoc(int docID, int freq) throws IOException;

  /** Add a new position & payload, and start/end offset.  A
   *  null payload means no payload; a non-null payload with
   *  zero length also means no payload.  Caller may reuse
   *  the {@link BytesRef} for the payload between calls
   *  (method must fully consume the payload). <code>startOffset</code>
   *  and <code>endOffset</code> will be -1 when offsets are not indexed. */
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException;

  /** Called when we are done adding positions & payloads
   *  for each doc. */
  public abstract void finishDoc() throws IOException;
}
