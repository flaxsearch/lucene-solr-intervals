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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.ExternalPaths;
import org.junit.Test;

import java.io.File;


/**
 *
 * @since solr 1.3
 */
public abstract class MultiCoreExampleTestBase extends SolrExampleTestBase 
{
  protected CoreContainer cores;

  private File dataDir2;
  private File dataDir1;

  @Override public String getSolrHome() { return ExternalPaths.EXAMPLE_MULTICORE_HOME; }

  protected void setupCoreContainer() {
    cores = new CoreContainer();
    cores.load();
  }
  
  @Override public void setUp() throws Exception {
    super.setUp();

    dataDir1 = new File(TEMP_DIR, getClass().getName() + "-core0-"
        + System.currentTimeMillis());
    dataDir1.mkdirs();
    
    dataDir2 = new File(TEMP_DIR, getClass().getName() + "-core1-"
        + System.currentTimeMillis());
    dataDir2.mkdirs();
    
    System.setProperty( "solr.core0.data.dir", this.dataDir1.getCanonicalPath() ); 
    System.setProperty( "solr.core1.data.dir", this.dataDir2.getCanonicalPath() );

    setupCoreContainer();

    SolrCore.log.info("CORES=" + cores + " : " + cores.getCoreNames());
    cores.setPersistent(false);


  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    
    String skip = System.getProperty("solr.test.leavedatadir");
    if (null != skip && 0 != skip.trim().length()) {
      System.err.println("NOTE: per solr.test.leavedatadir, dataDir2 will not be removed: " + dataDir2.getAbsolutePath());
    } else {
      if (!recurseDelete(dataDir2)) {
        System.err.println("!!!! WARNING: best effort to remove " + dataDir2.getAbsolutePath() + " FAILED !!!!!");
      }
    }

    cores.shutdown();
  }

  @Override
  protected final SolrServer getSolrServer()
  {
    throw new UnsupportedOperationException();
  }
  
  @Override
  protected final SolrServer createNewSolrServer()
  {
    throw new UnsupportedOperationException();
  }

  protected abstract SolrServer getSolrCore0();
  protected abstract SolrServer getSolrCore1();
  protected abstract SolrServer getSolrAdmin();
  protected abstract SolrServer getSolrCore(String name);

  @Test
  public void testMultiCore() throws Exception
  {
    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.deleteByQuery("*:*");
    up.process(getSolrCore0());
    up.process(getSolrCore1());
    up.clear();
    
    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField( "id", "AAA" );
    doc.setField( "name", "AAA1" );
    doc.setField( "type", "BBB1" );
    doc.setField( "core0", "yup" );
   
    // Add to core0
    up.add( doc );
    up.process( getSolrCore0() );

    // You can't add it to core1
    try {
      ignoreException("unknown field");
      up.process( getSolrCore1() );
      fail( "Can't add core0 field to core1!" );
    }
    catch( Exception ex ) {}
    resetExceptionIgnores();

    // Add to core1
    doc.setField( "id", "BBB" );
    doc.setField( "name", "BBB1" );
    doc.setField( "type", "AAA1" );
    doc.setField( "core1", "yup" );
    doc.removeField( "core0" );
    up.add( doc );
    up.process( getSolrCore1() );

    // You can't add it to core1
    try {
      ignoreException("unknown field");
      up.process( getSolrCore0() );
      fail( "Can't add core1 field to core0!" );
    }
    catch( Exception ex ) {}
    resetExceptionIgnores();

    // in core0
    doc = new SolrInputDocument();
    doc.setField( "id", "BBB1" );
    doc.setField( "name", "AAA1" );
    doc.setField( "type", "BBB" );
    doc.setField( "core0", "AAA1" );

    up.clear();
    up.add( doc );
    up.process( getSolrCore0() );

    // now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest( q );
    q.setQuery( "id:AAA" );
    assertEquals( 1, r.process( getSolrCore0() ).getResults().size() );
    assertEquals( 0, r.process( getSolrCore1() ).getResults().size() );
    
    // Now test Changing the default core
    assertEquals( 1, getSolrCore0().query( new SolrQuery( "id:AAA" ) ).getResults().size() );
    assertEquals( 0, getSolrCore0().query( new SolrQuery( "id:BBB" ) ).getResults().size() );

    assertEquals( 0, getSolrCore1().query( new SolrQuery( "id:AAA" ) ).getResults().size() );
    assertEquals( 1, getSolrCore1().query( new SolrQuery( "id:BBB" ) ).getResults().size() );

    // cross-core join
    assertEquals( 0, getSolrCore0().query( new SolrQuery( "{!join from=type to=name}*:*" ) ).getResults().size() );  // normal join
    assertEquals( 2, getSolrCore0().query( new SolrQuery( "{!join from=type to=name fromIndex=core1}id:BBB" ) ).getResults().size() );
    assertEquals( 1, getSolrCore1().query( new SolrQuery( "{!join from=type to=name fromIndex=core0}id:AAA" ) ).getResults().size() );

    // test that no rewrite happens in core0 (if it does, it will rewrite to BBB1 and nothing will be found in core1)
    assertEquals( 2, getSolrCore0().query( new SolrQuery( "{!join from=type to=name fromIndex=core1}id:BB~" ) ).getResults().size() );

    // test that query is parsed in the fromCore
    assertEquals( 2, getSolrCore0().query( new SolrQuery( "{!join from=type to=name fromIndex=core1}core1:yup" ) ).getResults().size() );

    // Now test reloading it should have a newer open time
    String name = "core0";
    SolrServer coreadmin = getSolrAdmin();
    CoreAdminResponse mcr = CoreAdminRequest.getStatus( name, coreadmin );
    long before = mcr.getStartTime( name ).getTime();
    CoreAdminRequest.reloadCore( name, coreadmin );
    
    // core should still have docs
    assertEquals( 1, getSolrCore0().query( new SolrQuery( "id:AAA" ) ).getResults().size() );
    
    mcr = CoreAdminRequest.getStatus( name, coreadmin );
    long after = mcr.getStartTime( name ).getTime();
    assertTrue( "should have more recent time: "+after+","+before, after > before );

    // test move
    CoreAdminRequest.renameCore("core1","corea",coreadmin);
    CoreAdminRequest.renameCore("corea","coreb",coreadmin);
    CoreAdminRequest.renameCore("coreb","corec",coreadmin);
    CoreAdminRequest.renameCore("corec","cored",coreadmin);
    CoreAdminRequest.renameCore("cored","corefoo",coreadmin);
    try {
      getSolrCore("core1").query( new SolrQuery( "id:BBB" ) );
      fail( "core1 should be gone" );
    }
    catch( Exception ex ) {}
    assertEquals( 1, getSolrCore("corefoo").query( new SolrQuery( "id:BBB" ) ).getResults().size() );

    NamedList<Object> response = getSolrCore("corefoo").query(new SolrQuery().setRequestHandler("/admin/system")).getResponse();
    NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
    String indexDir = (String) ((NamedList<Object>) coreInfo.get("directory")).get("index");
    
    
    response = getSolrCore("core0").query(new SolrQuery().setRequestHandler("/admin/system")).getResponse();
    coreInfo = (NamedList<Object>) response.get("core");
    String dataDir = (String) ((NamedList<Object>) coreInfo.get("directory")).get("data");
    

    System.out.println( (String) ((NamedList<Object>) coreInfo.get("directory")).get("dirimpl"));

    // test delete index on core
    CoreAdminRequest.unloadCore("corefoo", true, coreadmin);
    File dir = new File(indexDir);
    assertFalse("Index directory exists after core unload with deleteIndex=true", dir.exists());
    
    Unload req = new Unload(false);
    req.setDeleteDataDir(true);
    req.setCoreName("core0");
    req.process(coreadmin);
    
    dir = new File(dataDir);
    assertFalse("Data directory exists after core unload with deleteDataDir=true : " + dir, dir.exists());
  }
}
