package org.apache.lucene.store;

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

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.IOUtils;

/**
 * <p>Implements {@link LockFactory} using native OS file
 * locks.  Note that because this LockFactory relies on
 * java.nio.* APIs for locking, any problems with those APIs
 * will cause locking to fail.  Specifically, on certain NFS
 * environments the java.nio.* locks will fail (the lock can
 * incorrectly be double acquired) whereas {@link
 * SimpleFSLockFactory} worked perfectly in those same
 * environments.  For NFS based access to an index, it's
 * recommended that you try {@link SimpleFSLockFactory}
 * first and work around the one limitation that a lock file
 * could be left when the JVM exits abnormally.</p>
 *
 * <p>The primary benefit of {@link NativeFSLockFactory} is
 * that locks (not the lock file itsself) will be properly
 * removed (by the OS) if the JVM has an abnormal exit.</p>
 * 
 * <p>Note that, unlike {@link SimpleFSLockFactory}, the existence of
 * leftover lock files in the filesystem is fine because the OS
 * will free the locks held against these files even though the
 * files still remain. Lucene will never actively remove the lock
 * files, so although you see them, the index may not be locked.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change on all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 * 
 * <p>This is a singleton, you have to use {@link #INSTANCE}.
 *
 * @see LockFactory
 */

public final class NativeFSLockFactory extends FSLockFactory {
  
  /**
   * Singleton instance
   */
  public static final NativeFSLockFactory INSTANCE = new NativeFSLockFactory();

  private NativeFSLockFactory() {}

  @Override
  protected Lock makeFSLock(FSDirectory dir, String lockName) {
    return new NativeFSLock(dir.getDirectory(), lockName);
  }
  
  static final class NativeFSLock extends Lock {

    private FileChannel channel;
    private FileLock lock;
    private Path path;
    private Path lockDir;
    private static final Set<String> LOCK_HELD = Collections.synchronizedSet(new HashSet<String>());


    public NativeFSLock(Path lockDir, String lockFileName) {
      this.lockDir = lockDir;
      path = lockDir.resolve(lockFileName);
    }


    @Override
    public synchronized boolean obtain() throws IOException {

      if (lock != null) {
        // Our instance is already locked:
        return false;
      }

      // Ensure that lockDir exists and is a directory.
      Files.createDirectories(lockDir);
      try {
        Files.createFile(path);
      } catch (IOException ignore) {
        // we must create the file to have a truly canonical path.
        // if it's already created, we don't care. if it cant be created, it will fail below.
      }
      final Path canonicalPath = path.toRealPath();
      // Make sure nobody else in-process has this lock held
      // already, and, mark it held if not:
      // This is a pretty crazy workaround for some documented
      // but yet awkward JVM behavior:
      //
      //   On some systems, closing a channel releases all locks held by the Java virtual machine on the underlying file
      //   regardless of whether the locks were acquired via that channel or via another channel open on the same file.
      //   It is strongly recommended that, within a program, a unique channel be used to acquire all locks on any given
      //   file.
      //
      // This essentially means if we close "A" channel for a given file all locks might be released... the odd part
      // is that we can't re-obtain the lock in the same JVM but from a different process if that happens. Nevertheless
      // this is super trappy. See LUCENE-5738
      boolean obtained = false;
      if (LOCK_HELD.add(canonicalPath.toString())) {
        try {
          channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
          try {
            lock = channel.tryLock();
            obtained = lock != null;
          } catch (IOException | OverlappingFileLockException e) {
            // At least on OS X, we will sometimes get an
            // intermittent "Permission Denied" IOException,
            // which seems to simply mean "you failed to get
            // the lock".  But other IOExceptions could be
            // "permanent" (eg, locking is not supported via
            // the filesystem).  So, we record the failure
            // reason here; the timeout obtain (usually the
            // one calling us) will use this as "root cause"
            // if it fails to get the lock.
            failureReason = e;
          }
        } finally {
          if (obtained == false) { // not successful - clear up and move out
            clearLockHeld(path);
            final FileChannel toClose = channel;
            channel = null;
            IOUtils.closeWhileHandlingException(toClose);
          }
        }
      }
      return obtained;
    }

    @Override
    public synchronized void close() throws IOException {
      try {
        if (lock != null) {
          try {
            lock.release();
            lock = null;
          } finally {
            clearLockHeld(path);
          }
        }
      } finally {
        IOUtils.close(channel);
        channel = null;
      }
    }

    private static final void clearLockHeld(Path path) throws IOException {
      path = path.toRealPath();
      boolean remove = LOCK_HELD.remove(path.toString());
      assert remove : "Lock was cleared but never marked as held";
    }

    @Override
    public synchronized boolean isLocked() {
      // The test for is isLocked is not directly possible with native file locks:
      
      // First a shortcut, if a lock reference in this instance is available
      if (lock != null) return true;
      
      // Look if lock file is definitely not present; if not, there can definitely be no lock!
      if (Files.notExists(path)) return false;
      
      // Try to obtain and release (if was locked) the lock
      try {
        boolean obtained = obtain();
        if (obtained) close();
        return !obtained;
      } catch (IOException ioe) {
        return false;
      }    
    }

    @Override
    public String toString() {
      return "NativeFSLock@" + path;
    }
  }

}
