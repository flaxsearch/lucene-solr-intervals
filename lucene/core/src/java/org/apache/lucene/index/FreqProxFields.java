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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.util.AttributeSource; // javadocs
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Implements limited (iterators only, no stats) {@link
 *  Fields} interface over the in-RAM buffered
 *  fields/terms/postings, to flush postings through the
 *  PostingsFormat. */

class FreqProxFields extends Fields {
  final Map<String,FreqProxTermsWriterPerField> fields = new LinkedHashMap<String,FreqProxTermsWriterPerField>();

  public FreqProxFields(List<FreqProxTermsWriterPerField> fieldList) {
    // NOTE: fields are already sorted by field name
    for(FreqProxTermsWriterPerField field : fieldList) {
      fields.put(field.fieldInfo.name, field);
    }
  }

  public Iterator<String> iterator() {
    return fields.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    FreqProxTermsWriterPerField perField = fields.get(field);
    return perField == null ? null : new FreqProxTerms(perField);
  }

  @Override
  public int size() {
    //return fields.size();
    throw new UnsupportedOperationException();
  }

  private static class FreqProxTerms extends Terms {
    final FreqProxTermsWriterPerField terms;

    public FreqProxTerms(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) {
      FreqProxTermsEnum termsEnum;
      if (reuse instanceof FreqProxTermsEnum && ((FreqProxTermsEnum) reuse).terms == this.terms) {
        termsEnum = (FreqProxTermsEnum) reuse;
      } else {
        termsEnum = new FreqProxTermsEnum(terms);
      }
      termsEnum.reset();
      return termsEnum;
    }

    @Override
    public long size() {
      //return terms.termsHashPerField.bytesHash.size();
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      //return terms.sumTotalTermFreq;
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      //return terms.sumDocFreq;
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      //return terms.docCount;
      throw new UnsupportedOperationException();
    }
  
    @Override
    public boolean hasOffsets() {
      // NOTE: the in-memory buffer may have indexed offsets
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;      
    }
  
    @Override
    public boolean hasPositions() {
      // NOTE: the in-memory buffer may have indexed positions
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }
  
    @Override
    public boolean hasPayloads() {
      return terms.hasPayloads;
    }
  }

  private static class FreqProxTermsEnum extends TermsEnum {
    final FreqProxTermsWriterPerField terms;
    final int[] sortedTermIDs;
    final FreqProxPostingsArray postingsArray;
    final BytesRef scratch = new BytesRef();
    final int numTerms;
    int ord;

    public FreqProxTermsEnum(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
      this.numTerms = terms.termsHashPerField.bytesHash.size();
      sortedTermIDs = terms.sortedTermIDs;
      assert sortedTermIDs != null;
      postingsArray = (FreqProxPostingsArray) terms.termsHashPerField.postingsArray;
    }

    public void reset() {
      ord = -1;
    }

    public SeekStatus seekCeil(BytesRef text) {

      // TODO: we could instead keep the BytesRefHash
      // intact so this is a hash lookup

      // binary search:
      int lo = 0;
      int hi = numTerms - 1;
      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;
        int textStart = postingsArray.textStarts[sortedTermIDs[mid]];
        terms.termsHashPerField.bytePool.setBytesRef(scratch, textStart);
        int cmp = scratch.compareTo(text);
        if (cmp < 0) {
          lo = mid + 1;
        } else if (cmp > 0) {
          hi = mid - 1;
        } else {
          // found:
          ord = mid;
          return SeekStatus.FOUND;
        }
      }

      // not found:
      ord = lo + 1;
      if (ord == numTerms) {
        return SeekStatus.END;
      } else {
        return SeekStatus.NOT_FOUND;
      }
    }

    public void seekExact(long ord) {
      this.ord = (int) ord;
      int textStart = postingsArray.textStarts[sortedTermIDs[this.ord]];
      terms.termsHashPerField.bytePool.setBytesRef(scratch, textStart);
    }

    @Override
    public BytesRef next() {
      ord++;
      if (ord >= numTerms) {
        return null;
      } else {
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.termsHashPerField.bytePool.setBytesRef(scratch, textStart);
        return scratch;
      }
    }

    @Override
    public BytesRef term() {
      return scratch;
    }

    @Override
    public long ord() {
      return ord;
    }

    @Override
    public int docFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
      if (liveDocs != null) {
        throw new IllegalArgumentException("liveDocs must be null");
      }

      FreqProxDocsEnum docsEnum;

      if (!terms.hasFreq && (flags & DocsEnum.FLAG_FREQS) != 0) {
        // Caller wants freqs but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index freq");
      }

      if (reuse instanceof FreqProxDocsEnum) {
        docsEnum = (FreqProxDocsEnum) reuse;
        if (docsEnum.postingsArray != postingsArray) {
          docsEnum = new FreqProxDocsEnum(terms, postingsArray);
        }
      } else {
        docsEnum = new FreqProxDocsEnum(terms, postingsArray);
      }
      docsEnum.reset(sortedTermIDs[ord]);
      return docsEnum;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
      if (liveDocs != null) {
        throw new IllegalArgumentException("liveDocs must be null");
      }
      FreqProxDocsAndPositionsEnum posEnum;

      if (!terms.hasProx) {
        // Caller wants positions but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index positions");
      }

      if (!terms.hasOffsets && (flags & DocsAndPositionsEnum.FLAG_OFFSETS) != 0) {
        // Caller wants offsets but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index offsets");
      }

      if (reuse instanceof FreqProxDocsAndPositionsEnum) {
        posEnum = (FreqProxDocsAndPositionsEnum) reuse;
        if (posEnum.postingsArray != postingsArray) {
          posEnum = new FreqProxDocsAndPositionsEnum(terms, postingsArray);
        }
      } else {
        posEnum = new FreqProxDocsAndPositionsEnum(terms, postingsArray);
      }
      posEnum.reset(sortedTermIDs[ord]);
      return posEnum;
    }

    /**
     * Expert: Returns the TermsEnums internal state to position the TermsEnum
     * without re-seeking the term dictionary.
     * <p>
     * NOTE: A seek by {@link TermState} might not capture the
     * {@link AttributeSource}'s state. Callers must maintain the
     * {@link AttributeSource} states separately
     * 
     * @see TermState
     * @see #seekExact(BytesRef, TermState)
     */
    public TermState termState() throws IOException {
      return new TermState() {
        @Override
        public void copyFrom(TermState other) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private static class FreqProxDocsEnum extends DocsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader();
    final boolean readTermFreq;
    int docID;
    int freq;
    boolean ended;
    int termID;

    public FreqProxDocsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readTermFreq = terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.termsHashPerField.initReader(reader, termID, 0);
      ended = false;
      docID = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      // Don't lie here ... don't want codecs writings lots
      // of wasted 1s into the index:
      if (!readTermFreq) {
        throw new IllegalStateException("freq was not indexed");
      } else {
        return freq;
      }
    }

    @Override
    public int nextDoc() throws IOException {
      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          docID = postingsArray.lastDocIDs[termID];
          if (readTermFreq) {
            freq = postingsArray.termFreqs[termID];
          }
        }
      } else {
        int code = reader.readVInt();
        if (!readTermFreq) {
          docID += code;
        } else {
          docID += code >>> 1;
          if ((code & 1) != 0) {
            freq = 1;
          } else {
            freq = reader.readVInt();
          }
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FreqProxDocsAndPositionsEnum extends DocsAndPositionsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader();
    final ByteSliceReader posReader = new ByteSliceReader();
    final boolean readOffsets;
    int docID;
    int freq;
    int pos;
    int startOffset;
    int endOffset;
    int posLeft;
    int termID;
    boolean ended;
    boolean hasPayload;
    BytesRef payload = new BytesRef();

    public FreqProxDocsAndPositionsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readOffsets = terms.hasOffsets;
      assert terms.hasProx;
      assert terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.termsHashPerField.initReader(reader, termID, 0);
      terms.termsHashPerField.initReader(posReader, termID, 1);
      ended = false;
      docID = 0;
      posLeft = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextDoc() throws IOException {
      while (posLeft != 0) {
        nextPosition();
      }

      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          docID = postingsArray.lastDocIDs[termID];
          freq = postingsArray.termFreqs[termID];
        }
      } else {
        int code = reader.readVInt();
        docID += code >>> 1;
        if ((code & 1) != 0) {
          freq = 1;
        } else {
          freq = reader.readVInt();
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      posLeft = freq;
      pos = 0;
      startOffset = 0;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      assert posLeft > 0;
      posLeft--;
      int code = posReader.readVInt();
      pos += code >>> 1;
      if ((code & 1) != 0) {
        hasPayload = true;
        // has a payload
        payload.length = posReader.readVInt();
        if (payload.bytes.length < payload.length) {
          payload.grow(payload.length);
        }
        posReader.readBytes(payload.bytes, 0, payload.length);
      } else {
        hasPayload = false;
      }

      if (readOffsets) {
        startOffset += posReader.readVInt();
        endOffset = startOffset + posReader.readVInt();
      }

      return pos;
    }

    @Override
    public int startOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return startOffset;
    }

    @Override
    public int endOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (hasPayload) {
        return payload;
      } else {
        return null;
      }
    }
  }
}
