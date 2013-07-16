package org.apache.solr.core;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.store.blockcache.BlockDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.store.hdfs.HdfsLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String, DirContext)}.
 * 
 * This is an expert class and these API's are subject to change.
 * 
 */
public abstract class CachingDirectoryFactory extends DirectoryFactory {
  protected class CacheValue {
    final public String path;
    final public Directory directory;
    // for debug
    //final Exception originTrace;
    // use the setter!
    private boolean deleteOnClose = false;
    
    public CacheValue(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
      this.closeEntries.add(this);
      // for debug
      // this.originTrace = new RuntimeException("Originated from:");
    }
    public int refCnt = 1;
    // has doneWithDirectory(Directory) been called on this?
    public boolean closeCacheValueCalled = false;
    public boolean doneWithDir = false;
    private boolean deleteAfterCoreClose = false;
    public Set<CacheValue> removeEntries = new HashSet<CacheValue>();
    public Set<CacheValue> closeEntries = new HashSet<CacheValue>();

    public void setDeleteOnClose(boolean deleteOnClose, boolean deleteAfterCoreClose) {
      if (deleteOnClose) {
        removeEntries.add(this);
      }
      this.deleteOnClose = deleteOnClose;
      this.deleteAfterCoreClose = deleteAfterCoreClose;
    }
    
    @Override
    public String toString() {
      return "CachedDir<<" + "refCount=" + refCnt + ";path=" + path + ";done=" + doneWithDir + ">>";
    }
  }
  
  private static Logger log = LoggerFactory
      .getLogger(CachingDirectoryFactory.class);
  
  protected Map<String,CacheValue> byPathCache = new HashMap<String,CacheValue>();
  
  protected Map<Directory,CacheValue> byDirectoryCache = new IdentityHashMap<Directory,CacheValue>();
  
  protected Map<Directory,List<CloseListener>> closeListeners = new HashMap<Directory,List<CloseListener>>();
  
  protected Set<CacheValue> removeEntries = new HashSet<CacheValue>();

  private Double maxWriteMBPerSecFlush;

  private Double maxWriteMBPerSecMerge;

  private Double maxWriteMBPerSecRead;

  private Double maxWriteMBPerSecDefault;

  private boolean closed;
  
  public interface CloseListener {
    public void postClose();

    public void preClose();
  }
  
  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {
    synchronized (this) {
      if (!byDirectoryCache.containsKey(dir)) {
        throw new IllegalArgumentException("Unknown directory: " + dir
            + " " + byDirectoryCache);
      }
      List<CloseListener> listeners = closeListeners.get(dir);
      if (listeners == null) {
        listeners = new ArrayList<CloseListener>();
        closeListeners.put(dir, listeners);
      }
      listeners.add(closeListener);
      
      closeListeners.put(dir, listeners);
    }
  }
  
  @Override
  public void doneWithDirectory(Directory directory) throws IOException {
    synchronized (this) {
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory
            + " " + byDirectoryCache);
      }
      cacheValue.doneWithDir = true;
      log.debug("Done with dir: {}", cacheValue);
      if (cacheValue.refCnt == 0 && !closed) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      log.info("Closing " + this.getClass().getSimpleName() + " - " + byDirectoryCache.size() + " directories currently being tracked");
      this.closed = true;
      Collection<CacheValue> values = byDirectoryCache.values();
      for (CacheValue val : values) {
        log.debug("Closing {} - currently tracking: {}", 
                  this.getClass().getSimpleName(), val);
        try {
          // if there are still refs out, we have to wait for them
          int cnt = 0;
          while(val.refCnt != 0) {
            wait(100);
            
            if (cnt++ >= 120) {
              String msg = "Timeout waiting for all directory ref counts to be released - gave up waiting on " + val;
              log.error(msg);
              // debug
              // val.originTrace.printStackTrace();
              throw new SolrException(ErrorCode.SERVER_ERROR, msg);
            }
          }
          assert val.refCnt == 0 : val.refCnt;
        } catch (Throwable t) {
          SolrException.log(log, "Error closing directory", t);
        }
      }
      
      values = byDirectoryCache.values();
      Set<CacheValue> closedDirs = new HashSet<CacheValue>();
      for (CacheValue val : values) {
        try {
          for (CacheValue v : val.closeEntries) {
            assert v.refCnt == 0 : val.refCnt;
            log.debug("Closing directory when closing factory: " + v.path);
            boolean cl = closeCacheValue(v);
            if (cl) {
              closedDirs.add(v);
            }
          }
        } catch (Throwable t) {
          SolrException.log(log, "Error closing directory", t);
        }
      }

      for (CacheValue val : removeEntries) {
        log.info("Removing directory after core close: " + val.path);
        try {
          removeDirectory(val);
        } catch (Throwable t) {
          SolrException.log(log, "Error removing directory", t);
        }
      }
      
      for (CacheValue v : closedDirs) {
        removeFromCache(v);
      }
    }
  }

  private void removeFromCache(CacheValue v) {
    log.debug("Removing from cache: {}", v);
    byDirectoryCache.remove(v.directory);
    byPathCache.remove(v.path);
  }

  // be sure this is called with the this sync lock
  // returns true if we closed the cacheValue, false if it will be closed later
  private boolean closeCacheValue(CacheValue cacheValue) {
    log.info("looking to close " + cacheValue.path + " " + cacheValue.closeEntries.toString());
    List<CloseListener> listeners = closeListeners.remove(cacheValue.directory);
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.preClose();
        } catch (Throwable t) {
          SolrException.log(log, "Error executing preClose for directory", t);
        }
      }
    }
    cacheValue.closeCacheValueCalled = true;
    if (cacheValue.deleteOnClose) {
      // see if we are a subpath
      Collection<CacheValue> values = byPathCache.values();
      
      Collection<CacheValue> cacheValues = new ArrayList<CacheValue>(values);
      cacheValues.remove(cacheValue);
      for (CacheValue otherCacheValue : cacheValues) {
        // if we are a parent path and a sub path is not already closed, get a sub path to close us later
        if (isSubPath(cacheValue, otherCacheValue) && !otherCacheValue.closeCacheValueCalled) {
          // we let the sub dir remove and close us
          if (!otherCacheValue.deleteAfterCoreClose && cacheValue.deleteAfterCoreClose) {
            otherCacheValue.deleteAfterCoreClose = true;
          }
          otherCacheValue.removeEntries.addAll(cacheValue.removeEntries);
          otherCacheValue.closeEntries.addAll(cacheValue.closeEntries);
          cacheValue.closeEntries.clear();
          cacheValue.removeEntries.clear();
          return false;
        }
      }
    }

    boolean cl = false;
    for (CacheValue val : cacheValue.closeEntries) {
      close(val);
      if (val == cacheValue) {
        cl = true;
      }
    }

    for (CacheValue val : cacheValue.removeEntries) {
      if (!val.deleteAfterCoreClose) {
        log.info("Removing directory before core close: " + val.path);
        try {
          removeDirectory(val);
        } catch (Throwable t) {
          SolrException.log(log, "Error removing directory", t);
        }
      } else {
        removeEntries.add(val);
      }
    }
    
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.postClose();
        } catch (Throwable t) {
          SolrException.log(log, "Error executing postClose for directory", t);
        }
      }
    }
    return cl;
  }

  private void close(CacheValue val) {
    try {
      log.info("Closing directory: " + val.path);
      val.directory.close();
    } catch (Throwable t) {
      SolrException.log(log, "Error closing directory", t);
    }
  }

  private boolean isSubPath(CacheValue cacheValue, CacheValue otherCacheValue) {
    int one = cacheValue.path.lastIndexOf('/');
    int two = otherCacheValue.path.lastIndexOf('/');
    
    return otherCacheValue.path.startsWith(cacheValue.path + "/") && two > one;
  }

  @Override
  protected abstract Directory create(String path, DirContext dirContext) throws IOException;
  
  @Override
  public boolean exists(String path) throws IOException {
    // back compat behavior
    File dirFile = new File(path);
    return dirFile.canRead() && dirFile.list().length > 0;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path,  DirContext dirContext, String rawLockType)
      throws IOException {
    String fullPath = normalize(path);
    synchronized (this) {
      if (closed) {
        throw new AlreadyClosedException("Already closed");
      }
      
      final CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
      }
      
      if (directory == null) { 
        directory = create(fullPath, dirContext);
        
        directory = rateLimit(directory);
        
        CacheValue newCacheValue = new CacheValue(fullPath, directory);
        
        injectLockFactory(directory, fullPath, rawLockType);
        
        byDirectoryCache.put(directory, newCacheValue);
        byPathCache.put(fullPath, newCacheValue);
        log.info("return new directory for " + fullPath);
      } else {
        cacheValue.refCnt++;
        log.debug("Reusing cached directory: {}", cacheValue);
      }
      
      return directory;
    }
  }

  private Directory rateLimit(Directory directory) {
    if (maxWriteMBPerSecDefault != null || maxWriteMBPerSecFlush != null || maxWriteMBPerSecMerge != null || maxWriteMBPerSecRead != null) {
      directory = new RateLimitedDirectoryWrapper(directory);
      if (maxWriteMBPerSecDefault != null) {
        ((RateLimitedDirectoryWrapper)directory).setMaxWriteMBPerSec(maxWriteMBPerSecDefault, Context.DEFAULT);
      }
      if (maxWriteMBPerSecFlush != null) {
        ((RateLimitedDirectoryWrapper)directory).setMaxWriteMBPerSec(maxWriteMBPerSecFlush, Context.FLUSH);
      }
      if (maxWriteMBPerSecMerge != null) {
        ((RateLimitedDirectoryWrapper)directory).setMaxWriteMBPerSec(maxWriteMBPerSecMerge, Context.MERGE);
      }
      if (maxWriteMBPerSecRead != null) {
        ((RateLimitedDirectoryWrapper)directory).setMaxWriteMBPerSec(maxWriteMBPerSecRead, Context.READ);
      }
    }
    return directory;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.core.DirectoryFactory#incRef(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void incRef(Directory directory) {
    synchronized (this) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory);
      }
      
      cacheValue.refCnt++;
      log.debug("incRef'ed: {}", cacheValue);
    }
  }
  
  @Override
  public void init(NamedList args) {
    maxWriteMBPerSecFlush = (Double) args.get("maxWriteMBPerSecFlush");
    maxWriteMBPerSecMerge = (Double) args.get("maxWriteMBPerSecMerge");
    maxWriteMBPerSecRead = (Double) args.get("maxWriteMBPerSecRead");
    maxWriteMBPerSecDefault = (Double) args.get("maxWriteMBPerSecDefault");
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.core.DirectoryFactory#release(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void release(Directory directory) throws IOException {
    if (directory == null) {
      throw new NullPointerException();
    }
    synchronized (this) {
      // don't check if already closed here - we need to able to release
      // while #close() waits.
      
      CacheValue cacheValue = byDirectoryCache.get(directory);
      if (cacheValue == null) {
        throw new IllegalArgumentException("Unknown directory: " + directory
            + " " + byDirectoryCache);
      }
      log.debug("Releasing directory: " + cacheValue.path + " " + (cacheValue.refCnt - 1) + " " + cacheValue.doneWithDir);

      cacheValue.refCnt--;
      
      assert cacheValue.refCnt >= 0 : cacheValue.refCnt;

      if (cacheValue.refCnt == 0 && cacheValue.doneWithDir && !closed) {
        boolean cl = closeCacheValue(cacheValue);
        if (cl) {
          removeFromCache(cacheValue);
        }
      }
    }
  }
  
  @Override
  public void remove(String path) throws IOException {
    remove(path, false);
  }
  
  @Override
  public void remove(Directory dir) throws IOException {
    remove(dir, false);
  }
  
  @Override
  public void remove(String path, boolean deleteAfterCoreClose) throws IOException {
    synchronized (this) {
      CacheValue val = byPathCache.get(normalize(path));
      if (val == null) {
        throw new IllegalArgumentException("Unknown directory " + path);
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }
  
  @Override
  public void remove(Directory dir, boolean deleteAfterCoreClose) throws IOException {
    synchronized (this) {
      CacheValue val = byDirectoryCache.get(dir);
      if (val == null) {
        throw new IllegalArgumentException("Unknown directory " + dir);
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }
  
  private static Directory injectLockFactory(Directory dir, String lockPath,
      String rawLockType) throws IOException {
    if (null == rawLockType) {
      // we default to "simple" for backwards compatibility
      log.warn("No lockType configured for " + dir + " assuming 'simple'");
      rawLockType = "simple";
    }
    final String lockType = rawLockType.toLowerCase(Locale.ROOT).trim();
    
    if ("simple".equals(lockType)) {
      // multiple SimpleFSLockFactory instances should be OK
      dir.setLockFactory(new SimpleFSLockFactory(lockPath));
    } else if ("native".equals(lockType)) {
      dir.setLockFactory(new NativeFSLockFactory(lockPath));
    } else if ("single".equals(lockType)) {
      if (!(dir.getLockFactory() instanceof SingleInstanceLockFactory)) dir
          .setLockFactory(new SingleInstanceLockFactory());
    } else if ("hdfs".equals(lockType)) {
      Directory del = dir;
      
      if (dir instanceof NRTCachingDirectory) {
        del = ((NRTCachingDirectory) del).getDelegate();
      }
      
      if (del instanceof BlockDirectory) {
        del = ((BlockDirectory) del).getDirectory();
      }
      
      if (!(del instanceof HdfsDirectory)) {
        throw new SolrException(ErrorCode.FORBIDDEN, "Directory: "
            + del.getClass().getName()
            + ", but hdfs lock factory can only be used with HdfsDirectory");
      }

      dir.setLockFactory(new HdfsLockFactory(((HdfsDirectory)del).getHdfsDirPath(), ((HdfsDirectory)del).getConfiguration()));
    } else if ("none".equals(lockType)) {
      // Recipe for disaster
      log.error("CONFIGURATION WARNING: locks are disabled on " + dir);
      dir.setLockFactory(NoLockFactory.getNoLockFactory());
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unrecognized lockType: " + rawLockType);
    }
    return dir;
  }
  
  protected synchronized void removeDirectory(CacheValue cacheValue) throws IOException {
     // this page intentionally left blank
  }
  
  @Override
  public String normalize(String path) throws IOException {
    path = stripTrailingSlash(path);
    return path;
  }
  
  protected String stripTrailingSlash(String path) {
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }
  
  /**
   * Test only method for inspecting the cache
   * @return paths in the cache which have not been marked "done"
   *
   * @see #doneWithDirectory
   * @lucene.internal
   */
  public synchronized Set<String> getLivePaths() {
    HashSet<String> livePaths = new HashSet<String>();
    for (CacheValue val : byPathCache.values()) {
      if (!val.doneWithDir) {
        livePaths.add(val.path);
      }
    }
    return livePaths;
  }
}
