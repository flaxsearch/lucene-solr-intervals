package org.apache.lucene.search;

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
import java.io.PrintStream;

import org.apache.lucene.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene.document.DoubleField; // for javadocs
import org.apache.lucene.document.FloatField; // for javadocs
import org.apache.lucene.document.IntField; // for javadocs
import org.apache.lucene.document.LongField; // for javadocs
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Expert: Maintains caches of term values.
 *
 * <p>Created: May 19, 2004 11:13:14 AM
 *
 * @since   lucene 1.4
 * @see org.apache.lucene.util.FieldCacheSanityChecker
 *
 * @lucene.internal
 */
public interface FieldCache {

  /** Field values as 32-bit signed integers */
  public static abstract class Ints {
    /** Return an integer representation of this field's value. */
    public abstract int get(int docID);
    
    /** Zero value for every document */
    public static final Ints EMPTY = new Ints() {
      @Override
      public int get(int docID) {
        return 0;
      }
    };
  }

  /** Field values as 64-bit signed long integers */
  public static abstract class Longs {
    /** Return an long representation of this field's value. */
    public abstract long get(int docID);
    
    /** Zero value for every document */
    public static final Longs EMPTY = new Longs() {
      @Override
      public long get(int docID) {
        return 0;
      }
    };
  }

  /** Field values as 32-bit floats */
  public static abstract class Floats {
    /** Return an float representation of this field's value. */
    public abstract float get(int docID);
    
    /** Zero value for every document */
    public static final Floats EMPTY = new Floats() {
      @Override
      public float get(int docID) {
        return 0;
      }
    };
  }

  /** Field values as 64-bit doubles */
  public static abstract class Doubles {
    /** Return an double representation of this field's value. */
    public abstract double get(int docID);
    
    /** Zero value for every document */
    public static final Doubles EMPTY = new Doubles() {
      @Override
      public double get(int docID) {
        return 0;
      }
    };
  }
  
  /** Returns MISSING/-1 ordinal for every document */
  public static final SortedDocValues EMPTY_TERMSINDEX = new SortedDocValues() {
    @Override
    public int getOrd(int docID) {
      return -1;
    }

    @Override
    public void lookupOrd(int ord, BytesRef result) {
      result.bytes = MISSING;
      result.offset = 0;
      result.length = 0;
    }

    @Override
    public int getValueCount() {
      return 0;
    }
  };

  /**
   * Placeholder indicating creation of this cache is currently in-progress.
   */
  public static final class CreationPlaceholder {
    Object value;
  }

  /**
   * Marker interface as super-interface to all parsers. It
   * is used to specify a custom parser to {@link
   * SortField#SortField(String, FieldCache.Parser)}.
   */
  public interface Parser {
    
    /**
     * Pulls a {@link TermsEnum} from the given {@link Terms}. This method allows certain parsers
     * to filter the actual TermsEnum before the field cache is filled.
     * 
     * @param terms the {@link Terms} instance to create the {@link TermsEnum} from.
     * @return a possibly filtered {@link TermsEnum} instance, this method must not return <code>null</code>.
     * @throws IOException if an {@link IOException} occurs
     */
    public TermsEnum termsEnum(Terms terms) throws IOException;
  }

  /** Interface to parse ints from document fields.
   * @see FieldCache#getInts(AtomicReader, String, FieldCache.IntParser, boolean)
   */
  public interface IntParser extends Parser {
    /** Return an integer representation of this field's value. */
    public int parseInt(BytesRef term);
  }

  /** Interface to parse floats from document fields.
   * @see FieldCache#getFloats(AtomicReader, String, FieldCache.FloatParser, boolean)
   */
  public interface FloatParser extends Parser {
    /** Return an float representation of this field's value. */
    public float parseFloat(BytesRef term);
  }

  /** Interface to parse long from document fields.
   * @see FieldCache#getLongs(AtomicReader, String, FieldCache.LongParser, boolean)
   */
  public interface LongParser extends Parser {
    /** Return an long representation of this field's value. */
    public long parseLong(BytesRef term);
  }

  /** Interface to parse doubles from document fields.
   * @see FieldCache#getDoubles(AtomicReader, String, FieldCache.DoubleParser, boolean)
   */
  public interface DoubleParser extends Parser {
    /** Return an double representation of this field's value. */
    public double parseDouble(BytesRef term);
  }

  /** Expert: The cache used internally by sorting and range query classes. */
  public static FieldCache DEFAULT = new FieldCacheImpl();

  /**
   * A parser instance for int values encoded by {@link NumericUtils}, e.g. when indexed
   * via {@link IntField}/{@link NumericTokenStream}.
   */
  public static final IntParser NUMERIC_UTILS_INT_PARSER=new IntParser(){
    @Override
    public int parseInt(BytesRef term) {
      return NumericUtils.prefixCodedToInt(term);
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return NumericUtils.filterPrefixCodedInts(terms.iterator(null));
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_INT_PARSER"; 
    }
  };

  /**
   * A parser instance for float values encoded with {@link NumericUtils}, e.g. when indexed
   * via {@link FloatField}/{@link NumericTokenStream}.
   */
  public static final FloatParser NUMERIC_UTILS_FLOAT_PARSER=new FloatParser(){
    @Override
    public float parseFloat(BytesRef term) {
      return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(term));
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_FLOAT_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return NumericUtils.filterPrefixCodedInts(terms.iterator(null));
    }
  };

  /**
   * A parser instance for long values encoded by {@link NumericUtils}, e.g. when indexed
   * via {@link LongField}/{@link NumericTokenStream}.
   */
  public static final LongParser NUMERIC_UTILS_LONG_PARSER = new LongParser(){
    @Override
    public long parseLong(BytesRef term) {
      return NumericUtils.prefixCodedToLong(term);
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_LONG_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return NumericUtils.filterPrefixCodedLongs(terms.iterator(null));
    }
  };

  /**
   * A parser instance for double values encoded with {@link NumericUtils}, e.g. when indexed
   * via {@link DoubleField}/{@link NumericTokenStream}.
   */
  public static final DoubleParser NUMERIC_UTILS_DOUBLE_PARSER = new DoubleParser(){
    @Override
    public double parseDouble(BytesRef term) {
      return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term));
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_DOUBLE_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return NumericUtils.filterPrefixCodedLongs(terms.iterator(null));
    }
  };
  
 
  /** Checks the internal cache for an appropriate entry, and if none is found,
   *  reads the terms in <code>field</code> and returns a bit set at the size of
   *  <code>reader.maxDoc()</code>, with turned on bits for each docid that 
   *  does have a value for this field.  Note that if the field was only indexed
   *  as DocValues then this method will not work (it will return a Bits stating
   *  that no documents contain the field).
   */
  public Bits getDocsWithField(AtomicReader reader, String field) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public Ints getInts (AtomicReader reader, String field, boolean setDocsWithField) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as integers and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @param parser  Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public Ints getInts (AtomicReader reader, String field, IntParser parser, boolean setDocsWithField) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public Floats getFloats (AtomicReader reader, String field, boolean setDocsWithField) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @param parser  Computes float for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public Floats getFloats (AtomicReader reader, String field, FloatParser parser, boolean setDocsWithField) throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as longs and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws java.io.IOException If any error occurs.
   */
  public Longs getLongs(AtomicReader reader, String field, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as longs and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @param parser Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public Longs getLongs(AtomicReader reader, String field, LongParser parser, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public Doubles getDoubles(AtomicReader reader, String field, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as doubles and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @param parser Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public Doubles getDoubles(AtomicReader reader, String field, DoubleParser parser, boolean setDocsWithField) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link BinaryDocValues} instance, providing a
   * method to retrieve the term (as a BytesRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public BinaryDocValues getTerms (AtomicReader reader, String field)
  throws IOException;

  /** Expert: just like {@link #getTerms(AtomicReader,String)},
   *  but you can specify whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public BinaryDocValues getTerms (AtomicReader reader, String field, float acceptableOverheadRatio) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link SortedDocValues} instance,
   * providing methods to retrieve sort ordinals and terms
   * (as a ByteRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public SortedDocValues getTermsIndex (AtomicReader reader, String field) throws IOException;

  /** Expert: just like {@link
   *  #getTermsIndex(AtomicReader,String)}, but you can specify
   *  whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public SortedDocValues getTermsIndex (AtomicReader reader, String field, float acceptableOverheadRatio) throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found, reads the term values
   * in <code>field</code> and returns a {@link DocTermOrds} instance, providing a method to retrieve
   * the terms (as ords) per document.
   *
   * @param reader  Used to build a {@link DocTermOrds} instance
   * @param field   Which field contains the strings.
   * @return a {@link DocTermOrds} instance
   * @throws IOException  If any error occurs.
   */
  public SortedSetDocValues getDocTermOrds(AtomicReader reader, String field) throws IOException;

  /**
   * EXPERT: A unique Identifier/Description for each item in the FieldCache. 
   * Can be useful for logging/debugging.
   * @lucene.experimental
   */
  public final class CacheEntry {

    private final Object readerKey;
    private final String fieldName;
    private final Class<?> cacheType;
    private final Object custom;
    private final Object value;
    private String size;

    public CacheEntry(Object readerKey, String fieldName,
                      Class<?> cacheType,
                      Object custom,
                      Object value) {
      this.readerKey = readerKey;
      this.fieldName = fieldName;
      this.cacheType = cacheType;
      this.custom = custom;
      this.value = value;
    }

    public Object getReaderKey() {
      return readerKey;
    }

    public String getFieldName() {
      return fieldName;
    }

    public Class<?> getCacheType() {
      return cacheType;
    }

    public Object getCustom() {
      return custom;
    }

    public Object getValue() {
      return value;
    }

    /** 
     * Computes (and stores) the estimated size of the cache Value 
     * @see #getEstimatedSize
     */
    public void estimateSize() {
      long bytesUsed = RamUsageEstimator.sizeOf(getValue());
      size = RamUsageEstimator.humanReadableUnits(bytesUsed);
    }

    /**
     * The most recently estimated size of the value, null unless 
     * estimateSize has been called.
     */
    public String getEstimatedSize() {
      return size;
    }
    
    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("'").append(getReaderKey()).append("'=>");
      b.append("'").append(getFieldName()).append("',");
      b.append(getCacheType()).append(",").append(getCustom());
      b.append("=>").append(getValue().getClass().getName()).append("#");
      b.append(System.identityHashCode(getValue()));
      
      String s = getEstimatedSize();
      if(null != s) {
        b.append(" (size =~ ").append(s).append(')');
      }

      return b.toString();
    }
  }
  
  /**
   * EXPERT: Generates an array of CacheEntry objects representing all items 
   * currently in the FieldCache.
   * <p>
   * NOTE: These CacheEntry objects maintain a strong reference to the 
   * Cached Values.  Maintaining references to a CacheEntry the AtomicIndexReader 
   * associated with it has garbage collected will prevent the Value itself
   * from being garbage collected when the Cache drops the WeakReference.
   * </p>
   * @lucene.experimental
   */
  public abstract CacheEntry[] getCacheEntries();

  /**
   * <p>
   * EXPERT: Instructs the FieldCache to forcibly expunge all entries 
   * from the underlying caches.  This is intended only to be used for 
   * test methods as a way to ensure a known base state of the Cache 
   * (with out needing to rely on GC to free WeakReferences).  
   * It should not be relied on for "Cache maintenance" in general 
   * application code.
   * </p>
   * @lucene.experimental
   */
  public abstract void purgeAllCaches();

  /**
   * Expert: drops all cache entries associated with this
   * reader.  NOTE: this reader must precisely match the
   * reader that the cache entry is keyed on. If you pass a
   * top-level reader, it usually will have no effect as
   * Lucene now caches at the segment reader level.
   */
  public abstract void purge(AtomicReader r);

  /**
   * If non-null, FieldCacheImpl will warn whenever
   * entries are created that are not sane according to
   * {@link org.apache.lucene.util.FieldCacheSanityChecker}.
   */
  public void setInfoStream(PrintStream stream);

  /** counterpart of {@link #setInfoStream(PrintStream)} */
  public PrintStream getInfoStream();
}
