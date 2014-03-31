package org.apache.solr.handler.component;

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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;

/**
 * Test for QueryComponent's distributed querying optimization.
 * If the "fl" param is just "id" or just "id,score", all document data to return is already fetched by STAGE_EXECUTE_QUERY.
 * The second STAGE_GET_FIELDS query is completely unnecessary.
 * Eliminating that 2nd HTTP request can make a big difference in overall performance.
 *
 * @see QueryComponent
 */
public class DistributedQueryComponentOptimizationTest extends BaseDistributedSearchTestCase {

  public DistributedQueryComponentOptimizationTest() {
    fixShardCount = true;
    shardCount = 3;
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-custom-field.xml");
  }

  @Override
  public void doTest() throws Exception {
    del("*:*");

    index(id, "1", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x12, 0x62, 0x15 }),                     //  2
          // quick check to prove "*" dynamicField hasn't been broken by somebody mucking with schema
          "asdfasdf_field_should_match_catchall_dynamic_field_adsfasdf", "value");
    index(id, "2", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x16 }));                    //  5
    index(id, "3", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x32, 0x58 }));                    //  8
    index(id, "4", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x25, 0x21, 0x15 }));                    //  4
    index(id, "5", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x35, 0x35, 0x10, 0x00 }));              //  9
    index(id, "6", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03 }));  //  3
    index(id, "7", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x00, 0x3c, 0x73 }));                    //  1
    index(id, "8", "text", "c", "payload", ByteBuffer.wrap(new byte[] { 0x59, 0x2d, 0x4d }));                    // 11
    index(id, "9", "text", "a", "payload", ByteBuffer.wrap(new byte[] { 0x39, 0x79, 0x7a }));                    // 10
    index(id, "10", "text", "b", "payload", ByteBuffer.wrap(new byte[] { 0x31, 0x39, 0x7c }));                   //  6
    index(id, "11", "text", "d", "payload", ByteBuffer.wrap(new byte[] { (byte)0xff, (byte)0xaf, (byte)0x9c })); // 13
    index(id, "12", "text", "d", "payload", ByteBuffer.wrap(new byte[] { 0x34, (byte)0xdd, 0x4d }));             //  7
    index(id, "13", "text", "d", "payload", ByteBuffer.wrap(new byte[] { (byte)0x80, 0x11, 0x33 }));             // 12
    commit();

    handle.put("QTime", SKIPVAL);

    QueryResponse rsp;
    rsp = query("q", "*:*", "fl", "id,score", "sort", "payload asc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 7, 1, 6, 4, 2, 10, 12, 3, 5, 9, 8, 13, 11);
    rsp = query("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);
    // works with just fl=id as well
    rsp = query("q", "*:*", "fl", "id", "sort", "payload desc", "rows", "20");
    assertFieldValues(rsp.getResults(), id, 11, 13, 8, 9, 5, 3, 12, 10, 2, 4, 6, 1, 7);
  }
}
