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

package org.apache.solr.analytics.util.valuesource;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.solr.schema.TrieDateField;

/**
 * Extends {@link LongFieldSource} to have a field source that takes in 
 * and returns {@link Date} values while working with long values internally.
 */
public class DateFieldSource extends LongFieldSource {

  public DateFieldSource(String field) throws ParseException {
    super(field, null);
  }

  public DateFieldSource(String field, FieldCache.LongParser parser) {
    super(field, parser);
  }

  public long externalToLong(String extVal) {
    return parser.parseLong(new BytesRef(extVal));
  }

  public Object longToObject(long val) {
    return new Date(val);
  }

  @SuppressWarnings("deprecation")
  public String longToString(long val) {
    return TrieDateField.formatExternal((Date)longToObject(val));
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final FieldCache.Longs arr = cache.getLongs(readerContext.reader(), field, parser, true);
    final Bits valid = cache.getDocsWithField(readerContext.reader(), field);
    return new LongDocValues(this) {
      @Override
      public long longVal(int doc) {
        return arr.get(doc);
      }

      @Override
      public boolean exists(int doc) {
        return valid.get(doc);
      }

      @Override
      public Object objectVal(int doc) {
        return exists(doc) ? longToObject(arr.get(doc)) : null;
      }

      @Override
      public String strVal(int doc) {
        return exists(doc) ? longToString(arr.get(doc)) : null;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueDate mval = new MutableValueDate();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = arr.get(doc);
            mval.exists = exists(doc);
          }
        };
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != this.getClass()) return false;
    DateFieldSource other = (DateFieldSource) o;
    if (parser==null) {
      return field.equals(other.field);
    } else {
      return field.equals(other.field) && parser.equals(other.parser);
    }
  }

  @Override
  public int hashCode() {
    int h = parser == null ? this.getClass().hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }

}
