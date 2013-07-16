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
import org.apache.lucene.util.mutable.MutableValueDouble;
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
 * A legacy numeric field type that encodes "Double" values as Strings such 
 * that Term enumeration order matches the natural numeric order.  This class 
 * should not be used except by people with existing indexes that already 
 * contain fields of this type.  New schemas should use {@link TrieDoubleField}.
 *
 * <p>
 * The naming convention "Sortable" comes from the fact that both the numeric 
 * values and encoded String representations Sort identically (as opposed to 
 * a direct String representation where values such as "11" sort before values 
 * such as "2").
 * </p>
 * 
 * @see TrieDoubleField
 * @deprecated use {@link DoubleField} or {@link TrieDoubleField} - will be removed in 5.x
 */
@Deprecated
public class SortableDoubleField extends PrimitiveFieldType implements DoubleValueFieldType {
  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    return new SortableDoubleFieldSource(field.name);
  }

  @Override
  public String toInternal(String val) {
    return NumberUtils.double2sortableStr(val);
  }

  @Override
  public String toExternal(StorableField f) {
    return indexedToReadable(f.stringValue());
  }

  @Override
  public Double toObject(StorableField f) {
    return NumberUtils.SortableStr2double(f.stringValue());
  }
  
  @Override
  public String indexedToReadable(String indexedForm) {
    return NumberUtils.SortableStr2doubleStr(indexedForm);
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
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    String sval = f.stringValue();
    writer.writeDouble(name, NumberUtils.SortableStr2double(sval));
  }
}

class SortableDoubleFieldSource extends FieldCacheSource {
  protected double defVal;

  public SortableDoubleFieldSource(String field) {
    this(field, 0.0);
  }

  public SortableDoubleFieldSource(String field, double defVal) {
    super(field);
    this.defVal = defVal;
  }

  @Override
  public String description() {
    return "sdouble(" + field + ')';
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final double def = defVal;

    return new DocTermsIndexDocValues(this, readerContext, field) {
      private final BytesRef spare = new BytesRef();

      @Override
      protected String toTerm(String readableValue) {
        return NumberUtils.double2sortableStr(readableValue);
      }

      @Override
      public boolean exists(int doc) {
        return termsIndex.getOrd(doc) >= 0;
      }

      @Override
      public float floatVal(int doc) {
        return (float)doubleVal(doc);
      }

      @Override
      public int intVal(int doc) {
        return (int)doubleVal(doc);
      }

      @Override
      public long longVal(int doc) {
        return (long)doubleVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        int ord=termsIndex.getOrd(doc);
        if (ord == -1) {
          return def;
        } else {
          termsIndex.lookupOrd(ord, spare);
          return NumberUtils.SortableStr2double(spare);
        }
      }

      @Override
      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      @Override
      public Object objectVal(int doc) {
        return exists(doc) ? doubleVal(doc) : null;
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + doubleVal(doc);
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueDouble mval = new MutableValueDouble();

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
              mval.value = NumberUtils.SortableStr2double(spare);
              mval.exists = true;
            }
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SortableDoubleFieldSource
            && super.equals(o)
            && defVal == ((SortableDoubleFieldSource)o).defVal;
  }

  private static int hcode = SortableDoubleFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    long bits = Double.doubleToLongBits(defVal);
    int ibits = (int)(bits ^ (bits>>>32));  // mix upper bits into lower.
    return hcode + super.hashCode() + ibits;
  };
}





