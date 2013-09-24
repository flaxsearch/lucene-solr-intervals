package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.util.LuceneTestCase;

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

/** Read-write version of Lucene40Codec for testing */
@SuppressWarnings("deprecation")
public final class Lucene40RWCodec extends Lucene40Codec {
  
  private final FieldInfosFormat fieldInfos = new Lucene40FieldInfosFormat() {
    @Override
    public FieldInfosWriter getFieldInfosWriter() throws IOException {
      if (!LuceneTestCase.OLD_FORMAT_IMPERSONATION_IS_ACTIVE) {
        return super.getFieldInfosWriter();
      } else {
        return new Lucene40FieldInfosWriter();
      }
    }
  };
  
  private final DocValuesFormat docValues = new Lucene40RWDocValuesFormat();
  private final NormsFormat norms = new Lucene40RWNormsFormat();
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfos;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }

  @Override
  public NormsFormat normsFormat() {
    return norms;
  }
}
