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

package org.apache.solr.handler.admin;

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * This handler uses the RawResponseWriter to give client access to
 * files inside ${solr.home}/conf
 * <p>
 * If you want to selectively restrict access some configuration files, you can list
 * these files in the {@link #HIDDEN} invariants.  For example to hide 
 * synonyms.txt and anotherfile.txt, you would register:
 * <p>
 * <pre>
 * &lt;requestHandler name="/admin/file" class="org.apache.solr.handler.admin.ShowFileRequestHandler" &gt;
 *   &lt;lst name="defaults"&gt;
 *    &lt;str name="echoParams"&gt;explicit&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="invariants"&gt;
 *    &lt;str name="hidden"&gt;synonyms.txt&lt;/str&gt; 
 *    &lt;str name="hidden"&gt;anotherfile.txt&lt;/str&gt; 
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 * <p>
 * The ShowFileRequestHandler uses the {@link RawResponseWriter} (wt=raw) to return
 * file contents.  If you need to use a different writer, you will need to change 
 * the registered invariant param for wt.
 * <p>
 * If you want to override the contentType header returned for a given file, you can
 * set it directly using: {@link #USE_CONTENT_TYPE}.  For example, to get a plain text
 * version of schema.xml, try:
 * <pre>
 *   http://localhost:8983/solr/admin/file?file=schema.xml&contentType=text/plain
 * </pre>
 * 
 *
 * @since solr 1.3
 */
public class ShowFileRequestHandler extends RequestHandlerBase
{
  public static final String HIDDEN = "hidden";
  public static final String USE_CONTENT_TYPE = "contentType";
  
  protected Set<String> hiddenFiles;
  
  public ShowFileRequestHandler()
  {
    super();
  }

  @Override
  public void init(NamedList args) {
    super.init( args );

    // Build a list of hidden files
    hiddenFiles = new HashSet<String>();
    if( invariants != null ) {
      String[] hidden = invariants.getParams( HIDDEN );
      if( hidden != null ) {
        for( String s : hidden ) {
          hiddenFiles.add( s.toUpperCase(Locale.ROOT) );
        }
      }
    }
  }
  
  public Set<String> getHiddenFiles()
  {
    return hiddenFiles;
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, KeeperException, InterruptedException 
  {
    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    if (coreContainer.isZooKeeperAware()) {
      showFromZooKeeper(req, rsp, coreContainer);
    } else {
      showFromFileSystem(req, rsp);
    }
  }

  private void showFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp,
      CoreContainer coreContainer) throws KeeperException,
      InterruptedException, UnsupportedEncodingException {
    String adminFile = null;
    SolrCore core = req.getCore();
    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
    final ZkSolrResourceLoader loader = (ZkSolrResourceLoader) core
        .getResourceLoader();
    String confPath = loader.getCollectionZkPath();
    
    String fname = req.getParams().get("file", null);
    if (fname == null) {
      adminFile = confPath;
    } else {
      fname = fname.replace('\\', '/'); // normalize slashes
      if (hiddenFiles.contains(fname.toUpperCase(Locale.ROOT))) {
        rsp.setException(new SolrException(ErrorCode.FORBIDDEN, "Can not access: " + fname));
        return;
      }
      if (fname.indexOf("..") >= 0) {
        rsp.setException(new SolrException(ErrorCode.FORBIDDEN, "Invalid path: " + fname));
        return;
      }
      if (fname.startsWith("/")) { // Only files relative to conf are valid
        fname = fname.substring(1);
      }
      adminFile = confPath + "/" + fname;
    }
    
    // Make sure the file exists, is readable and is not a hidden file
    if (!zkClient.exists(adminFile, true)) {
      rsp.setException(new SolrException(ErrorCode.NOT_FOUND, "Can not find: "
                                         + adminFile));
      return;
    }
    
    // Show a directory listing
    List<String> children = zkClient.getChildren(adminFile, null, true);
    if (children.size() > 0) {
      
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
      for (String f : children) {
        if (hiddenFiles.contains(f.toUpperCase(Locale.ROOT))) {
          continue; // don't show 'hidden' files
        }
        if (f.startsWith(".")) {
          continue; // skip hidden system files...
        }
        
        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<Object>();
        files.add(f, fileInfo);
        List<String> fchildren = zkClient.getChildren(adminFile, null, true);
        if (fchildren.size() > 0) {
          fileInfo.add("directory", true);
        } else {
          // TODO? content type
          fileInfo.add("size", f.length());
        }
        // TODO: ?
        // fileInfo.add( "modified", new Date( f.lastModified() ) );
      }
      rsp.add("files", files);
    } else {
      // Include the file contents
      // The file logic depends on RawResponseWriter, so force its use.
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(CommonParams.WT, "raw");
      req.setParams(params);
      ContentStreamBase content = new ContentStreamBase.ByteArrayStream(zkClient.getData(adminFile, null, null, true), adminFile);
      content.setContentType(req.getParams().get(USE_CONTENT_TYPE));
      
      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }

  private void showFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) {
    File adminFile = null;
    
    final SolrResourceLoader loader = req.getCore().getResourceLoader();
    File configdir = new File( loader.getConfigDir() );
    if (!configdir.exists()) {
      // TODO: maybe we should just open it this way to start with?
      try {
        configdir = new File( loader.getClassLoader().getResource(loader.getConfigDir()).toURI() );
      } catch (URISyntaxException e) {
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Can not access configuration directory!", e));
        return;
      }
    }
    String fname = req.getParams().get("file", null);
    if( fname == null ) {
      adminFile = configdir;
    }
    else {
      fname = fname.replace( '\\', '/' ); // normalize slashes
      if( hiddenFiles.contains( fname.toUpperCase(Locale.ROOT) ) ) {
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Can not access: "+fname ));
        return;
      }
      if( fname.indexOf( ".." ) >= 0 ) {
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Invalid path: "+fname ));
        return;
      }
      adminFile = new File( configdir, fname );
    }
    
    // Make sure the file exists, is readable and is not a hidden file
    if( !adminFile.exists() ) {
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not find: "+adminFile.getName() 
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }
    if( !adminFile.canRead() || adminFile.isHidden() ) {
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not show: "+adminFile.getName() 
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }
    
    // Show a directory listing
    if( adminFile.isDirectory() ) {
      
      int basePath = configdir.getAbsolutePath().length() + 1;
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
      for( File f : adminFile.listFiles() ) {
        String path = f.getAbsolutePath().substring( basePath );
        path = path.replace( '\\', '/' ); // normalize slashes
        if( hiddenFiles.contains( path.toUpperCase(Locale.ROOT) ) ) {
          continue; // don't show 'hidden' files
        }
        if( f.isHidden() || f.getName().startsWith( "." ) ) {
          continue; // skip hidden system files...
        }
        
        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<Object>();
        files.add( path, fileInfo );
        if( f.isDirectory() ) {
          fileInfo.add( "directory", true ); 
        }
        else {
          // TODO? content type
          fileInfo.add( "size", f.length() );
        }
        fileInfo.add( "modified", new Date( f.lastModified() ) );
      }
      rsp.add( "files", files );
    }
    else {
      // Include the file contents
      //The file logic depends on RawResponseWriter, so force its use.
      ModifiableSolrParams params = new ModifiableSolrParams( req.getParams() );
      params.set( CommonParams.WT, "raw" );
      req.setParams(params);

      ContentStreamBase content = new ContentStreamBase.FileStream( adminFile );
      content.setContentType( req.getParams().get( USE_CONTENT_TYPE ) );

      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }
  
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Admin Get File -- view config files directly";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
