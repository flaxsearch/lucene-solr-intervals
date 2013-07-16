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

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.index.GeneralField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

import java.io.IOException;
import java.util.Map;
/**
 * A legacy numeric field type that encodes "Long" values as simple Strings.
 * This class should not be used except by people with existing indexes that
 * contain numeric values indexed as Strings.  
 * New schemas should use {@link TrieLongField}.
 *
 * <p>
 * Field values will sort numerically, but Range Queries (and other features 
 * that rely on numeric ranges) will not work as expected: values will be 
 * evaluated in unicode String order, not numeric order.
 * </p>
 * 
 * @see TrieLongField
 */
public class LongField extends PrimitiveFieldType implements LongValueFieldType {

  private static final FieldCache.LongParser PARSER = new FieldCache.LongParser() {
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return terms.iterator(null);
    }

    @Override
    public long parseLong(BytesRef term) {
      return Long.parseLong(term.utf8ToString());
    }
  };

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
  }

  /////////////////////////////////////////////////////////////

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    field.checkSortability();
    return new SortField(field.name, PARSER, reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    return new LongFieldSource(field.name, PARSER);
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    String s = f.stringValue();

    // these values may be from a legacy lucene index, which may
    // not be properly formatted in some output formats, or may
    // incorrectly have a zero length.

    if (s.length()==0) {
      // zero length value means someone mistakenly indexed the value
      // instead of simply leaving it out.  Write a null value instead of a numeric.
      writer.writeNull(name);
      return;
    }

    try {
      long val = Long.parseLong(s);
      writer.writeLong(name, val);
    } catch (NumberFormatException e){
      // can't parse - write out the contents as a string so nothing is lost and
      // clients don't get a parse error.
      writer.writeStr(name, s, true);
    }
  }

  @Override
  public Long toObject(StorableField f) {
    return Long.valueOf( toExternal(f) );
  }
}
