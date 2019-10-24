/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

/**
 * Test check access operation.
 */
public class ITestAzureBlobFileSystemCheckAccess
    extends AbstractAbfsIntegrationTest {

  private static final Path TEST_FOLDER_PATH = new Path(
      "CheckAccessTestFolder");

  public ITestAzureBlobFileSystemCheckAccess() throws Exception {
    super();
  }

  @Test(expected = NullPointerException.class)
  public void testCheckAccessWithNullPath() throws IOException {
    Assume.assumeTrue(
        ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    getFileSystem().access(null, FsAction.READ);
  }

  @Test(expected = NullPointerException.class)
  public void testCheckAccessForFileWithNullFsAction() throws IOException {
    Assume.assumeTrue(
        ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    FileSystem fs = getFileSystem();
    Path testFilePath = new Path(TEST_FOLDER_PATH, "childfile1");
    fs.create(testFilePath);
    fs.access(testFilePath, null);
  }

  @Test
  public void testCheckReadAccessForFileWithReadPermission()
      throws IOException {
    Assume.assumeTrue(
        ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    FileSystem fs = getFileSystem();
    Path testFilePath = new Path(TEST_FOLDER_PATH, "childfile2");
    FsPermission perm = new FsPermission(FsAction.READ, FsAction.READ,
        FsAction.READ, true);
    int bufferSize = 4 * 1024 * 1024;
    fs.create(testFilePath, perm, true, bufferSize,
        fs.getDefaultReplication(testFilePath),
        fs.getDefaultBlockSize(testFilePath), null);
    Exception ex = null;
    fs.access(testFilePath, FsAction.READ);
  }

  @Test(expected = AccessControlException.class)
  public void testCheckWriteAccessForFileWithReadPermission()
      throws IOException {
    Assume.assumeTrue(
        ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        getConfiguration().isCheckAccessEnabled());
    FileSystem fs = getFileSystem();
    Path testFilePath = new Path(TEST_FOLDER_PATH, "childfile3");
    FsPermission perm = new FsPermission(FsAction.READ, FsAction.READ,
        FsAction.READ, true);
    int bufferSize = 4 * 1024 * 1024;
    fs.create(testFilePath, perm, true, bufferSize,
        fs.getDefaultReplication(testFilePath),
        fs.getDefaultBlockSize(testFilePath), null);
    fs.access(testFilePath, FsAction.WRITE);
  }
}