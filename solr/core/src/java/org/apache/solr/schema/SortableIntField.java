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

package org.apache.solr.schema;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.solr.search.QParser;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.GeneralField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.response.TextResponseWriter;

import java.util.Map;
import java.io.IOException;

/**
 * A legacy numeric field type that encodes "Integer" values as Strings such 
 * that Term enumeration order matches the natural numeric order.  This class 
 * should not be used except by people with existing indexes that already 
 * contain fields of this type.  New schemas should use {@link TrieIntField}.
 *
 * <p>
 * The naming convention "Sortable" comes from the fact that both the numeric 
 * values and encoded String representations Sort identically (as opposed to 
 * a direct String representation where values such as "11" sort before values 
 * such as "2").
 * </p>
 * 
 * @see TrieIntField
 * @deprecated use {@link IntField} or {@link TrieIntField} - will be removed in 5.x
 */
@Deprecated
public class SortableIntField extends PrimitiveFieldType implements IntValueFieldType {
  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    return new SortableIntFieldSource(field.name);
  }

  @Override
  public String toInternal(String val) {
    // special case single digits?  years?, etc
    // stringCache?  general stringCache on a
    // global field level?
    return NumberUtils.int2sortableStr(val);
  }

  @Override
  public String toExternal(StorableField f) {
    return indexedToReadable(f.stringValue());
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2int(indexedForm);
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRef charsRef) {
    // TODO: this could be more efficient, but the sortable types should be deprecated instead
    UnicodeUtil.UTF8toUTF16(input, charsRef);
    final char[] indexedToReadable = indexedToReadable(charsRef.toString()).toCharArray();
    charsRef.copyChars(indexedToReadable, 0, indexedToReadable.length);
    return charsRef;
  }

  @Override
  public Integer toObject(StorableField f) {
    return NumberUtils.SortableStr2int(f.stringValue(), 0, 3);    
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    String sval = f.stringValue();
    writer.writeInt(name, NumberUtils.SortableStr2int(sval,0,sval.length()));
  }

  @Override
  public Object marshalSortValue(Object value) {
    if (null == value) { 
      return null;
    }
    CharsRef chars = new CharsRef();
    UnicodeUtil.UTF8toUTF16((BytesRef)value, chars);
    return NumberUtils.SortableStr2int(chars.toString());
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    if (null == value) {
      return null;
    }
    String sortableString = NumberUtils.int2sortableStr(value.toString());
    BytesRef bytes = new BytesRef();
    UnicodeUtil.UTF16toUTF8(sortableString, 0, sortableString.length(), bytes);
    return bytes;
  }
}



class SortableIntFieldSource extends FieldCacheSource {
  protected int defVal;

  public SortableIntFieldSource(String field) {
    this(field, 0);
  }

  public SortableIntFieldSource(String field, int defVal) {
    super(field);
    this.defVal = defVal;
  }

  @Override
  public String description() {
    return "sint(" + field + ')';
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final int def = defVal;

    return new DocTermsIndexDocValues(this, readerContext, field) {
      private final BytesRef spare = new BytesRef();

      @Override
      protected String toTerm(String readableValue) {
        return NumberUtils.int2sortableStr(readableValue);
      }

      @Override
      public float floatVal(int doc) {
        return (float)intVal(doc);
      }

      @Override
      public boolean exists(int doc) {
        return termsIndex.getOrd(doc) >= 0;
      }

      @Override
      public int intVal(int doc) {
        int ord=termsIndex.getOrd(doc);
        if (ord==-1) {
          return def;
        } else {
          termsIndex.lookupOrd(ord, spare);
          return NumberUtils.SortableStr2int(spare,0,3);
        }
      }

      @Override
      public long longVal(int doc) {
        return (long)intVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return (double)intVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return Integer.toString(intVal(doc));
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + intVal(doc);
      }

      @Override
      public Object objectVal(int doc) {
        return exists(doc) ? intVal(doc) : null;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueInt mval = new MutableValueInt();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            int ord=termsIndex.getOrd(doc);
            if (ord == -1) {
              mval.value = def;
              mval.exists = false;
            } else {
              termsIndex.lookupOrd(ord, spare);
              mval.value = NumberUtils.SortableStr2int(spare,0,3);
              mval.exists = true;
            }
          }
        };
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SortableIntFieldSource
            && super.equals(o)
            && defVal == ((SortableIntFieldSource)o).defVal;
  }

  private static int hcode = SortableIntFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + super.hashCode() + defVal;
  };
}
