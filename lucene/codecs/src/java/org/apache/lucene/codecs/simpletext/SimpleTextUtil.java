package org.apache.lucene.codecs.simpletext;

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
import java.util.Locale;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

class SimpleTextUtil {
  public final static byte NEWLINE = 10;
  public final static byte ESCAPE = 92;
  final static BytesRef CHECKSUM = new BytesRef("checksum ");
  
  public static void write(DataOutput out, String s, BytesRef scratch) throws IOException {
    UnicodeUtil.UTF16toUTF8(s, 0, s.length(), scratch);
    write(out, scratch);
  }

  public static void write(DataOutput out, BytesRef b) throws IOException {
    for(int i=0;i<b.length;i++) {
      final byte bx = b.bytes[b.offset+i];
      if (bx == NEWLINE || bx == ESCAPE) {
        out.writeByte(ESCAPE);
      }
      out.writeByte(bx);
    }
  }

  public static void writeNewline(DataOutput out) throws IOException {
    out.writeByte(NEWLINE);
  }
  
  public static void readLine(DataInput in, BytesRef scratch) throws IOException {
    int upto = 0;
    while(true) {
      byte b = in.readByte();
      if (scratch.bytes.length == upto) {
        scratch.grow(1+upto);
      }
      if (b == ESCAPE) {
        scratch.bytes[upto++] = in.readByte();
      } else {
        if (b == NEWLINE) {
          break;
        } else {
          scratch.bytes[upto++] = b;
        }
      }
    }
    scratch.offset = 0;
    scratch.length = upto;
  }

  public static void writeChecksum(IndexOutput out, BytesRef scratch) throws IOException {
    // Pad with zeros so different checksum values use the
    // same number of bytes
    // (BaseIndexFileFormatTestCase.testMergeStability cares):
    String checksum = String.format(Locale.ROOT, "%020d", out.getChecksum());
    SimpleTextUtil.write(out, CHECKSUM);
    SimpleTextUtil.write(out, checksum, scratch);
    SimpleTextUtil.writeNewline(out);
  }
  
  public static void checkFooter(ChecksumIndexInput input) throws IOException {
    BytesRef scratch = new BytesRef();
    String expectedChecksum = String.format(Locale.ROOT, "%020d", input.getChecksum());
    SimpleTextUtil.readLine(input, scratch);
    if (StringHelper.startsWith(scratch, CHECKSUM) == false) {
      throw new CorruptIndexException("SimpleText failure: expected checksum line but got " + scratch.utf8ToString() + " (resource=" + input + ")");
    }
    String actualChecksum = new BytesRef(scratch.bytes, CHECKSUM.length, scratch.length - CHECKSUM.length).utf8ToString();
    if (!expectedChecksum.equals(actualChecksum)) {
      throw new CorruptIndexException("SimpleText checksum failure: " + actualChecksum + " != " + expectedChecksum + " (resource=" + input + ")");
    }
    if (input.length() != input.getFilePointer()) {
      throw new CorruptIndexException("Unexpected stuff at the end of file, please be careful with your text editor! (resource=" + input + ")");
    }
  }
}
