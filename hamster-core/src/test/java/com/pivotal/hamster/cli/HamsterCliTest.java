package com.pivotal.hamster.cli;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.commons.cli.ParseException;

public class HamsterCliTest {
  static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  class MockHamsterCli extends HamsterCli {
    FileSystem injectFs = null;

    public MockHamsterCli(FileSystem fs) {
      this.injectFs = fs;
    }

    @Override
    void createYarnRPC() {
      this.client = new MockClientRMProtocol();
    }

    @Override
    FileSystem getRemoteFileSystem() throws IOException {
      if (null == injectFs) {
        throw new IOException("should call setFs before call this");
      }
      return injectFs;
    }

    @Override
    void processHamsterJar(Map<String, LocalResource> localResources,
        FileSystem fs, Path publicUploadPath) throws IOException {
      // do nothing, we don't have such jars
    }
  }

  /**
   * a mock byte array output stream that will put byte array to files when user
   * called close()
   */
  class WrappedByteArrayOutputStream extends ByteArrayOutputStream {
    Map<String, byte[]> files;
    String path;

    public WrappedByteArrayOutputStream(Map<String, byte[]> files, String path) {
      this.files = files;
      this.path = path;
    }

    @Override
    public void close() throws IOException {
      files.put(path, this.toByteArray());
      super.close();
    }
  }

  class MockFileSystem extends FileSystem {
    Map<String, byte[]> files = new HashMap<String, byte[]>();

    public MockFileSystem(Configuration conf) {
      this.setConf(conf);
    }

    @Override
    public URI getUri() {
      return URI.create("mockfs://localhost/");
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      if (files.containsKey(f.toString())) {
        byte[] content = files.get(f.toString());
        if (null == content) {
          content = new byte[0];
        }
        return new FSDataInputStream(new ByteArrayInputStream(content));
      }
      throw new IOException("no file file created:" + f.toString());
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      if (files.containsKey(f.toString()) && (!overwrite)) {
        throw new IOException("cannot create, file exists!");
      }
      files.put(f.toString(), null);
      return new FSDataOutputStream(new WrappedByteArrayOutputStream(files,
          f.toString()));
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      throw new IOException("not implemented");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      throw new IOException("not implemented");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      if (recursive) {
        for (String file : files.keySet()) {
          if (file.startsWith(f.toString())) {
            files.remove(file);
          }
        }
      } else {
        files.remove(f.toString());
      }
      return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      List<FileStatus> statuses = new ArrayList<FileStatus>();

      for (String file : files.keySet()) {
        if (file.startsWith(f.toString())
            && (!file.equalsIgnoreCase(f.toString()))) {
          byte[] content = files.get(file);
          if (null == content) {
            content = new byte[0];
          }
          statuses.add(new FileStatus(content.length, false, 1, 0, 123456,
              new Path(file)));
        }
      }

      return statuses.toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (!files.containsKey(f.toString())) {
        throw new FileNotFoundException("file not exist");
      }
      byte[] content = files.get(f.toString());
      if (null == content) {
        content = new byte[0];
      }
      FileStatus status = new FileStatus(content.length, false, 1, 0, 123456, f);
      return status;
    }

  }

  class MockClientRMProtocol implements ClientRMProtocol {
    YarnApplicationState yarnState = YarnApplicationState.NEW;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;

    public void setYarnApplicationState(YarnApplicationState state) {
      yarnState = state;
    }

    public void setFinalApplicationStatus(FinalApplicationStatus state) {
      finalStatus = state;
    }

    public GetNewApplicationResponse getNewApplication(
        GetNewApplicationRequest request) throws YarnRemoteException {
      GetNewApplicationResponse res = recordFactory
          .newRecordInstance(GetNewApplicationResponse.class);
      ApplicationId appId = recordFactory
          .newRecordInstance(ApplicationId.class);
      appId.setClusterTimestamp(123456L);
      appId.setId(123);
      res.setApplicationId(appId);
      res.setApplicationId(appId);
      return res;
    }

    public SubmitApplicationResponse submitApplication(
        SubmitApplicationRequest request) throws YarnRemoteException {
      SubmitApplicationResponse res = recordFactory
          .newRecordInstance(SubmitApplicationResponse.class);
      return res;
    }

    public KillApplicationResponse forceKillApplication(
        KillApplicationRequest request) throws YarnRemoteException {
      return null;
    }

    public GetApplicationReportResponse getApplicationReport(
        GetApplicationReportRequest request) throws YarnRemoteException {
      GetApplicationReportResponse res = recordFactory
          .newRecordInstance(GetApplicationReportResponse.class);
      ApplicationReport report = recordFactory
          .newRecordInstance(ApplicationReport.class);
      report.setFinalApplicationStatus(finalStatus);
      report.setYarnApplicationState(yarnState);
      res.setApplicationReport(report);
      return res;
    }

    public GetClusterMetricsResponse getClusterMetrics(
        GetClusterMetricsRequest request) throws YarnRemoteException {
      return null;
    }

    public GetAllApplicationsResponse getAllApplications(
        GetAllApplicationsRequest request) throws YarnRemoteException {
      return null;
    }

    public GetClusterNodesResponse getClusterNodes(
        GetClusterNodesRequest request) throws YarnRemoteException {
      return null;
    }

    public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
        throws YarnRemoteException {
      return null;
    }

    public GetQueueUserAclsInfoResponse getQueueUserAcls(
        GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
      return null;
    }

    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
        CancelDelegationTokenRequest arg0) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
        RenewDelegationTokenRequest arg0) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }
  }

  /**
   * Test for a full life of initialize client, new application and submit
   * application
   * 
   * @throws ParseException
   */
  @Test
  public void testWholeProcess() throws IOException, InterruptedException,
      ParseException {
    MockFileSystem fs = new MockFileSystem(new Configuration());
    HamsterCli cli = new MockHamsterCli(fs);
    final String HAMSTER_HOME = "/path/to/hamster";

    // set necessary properties
    cli.conf.setBoolean(HamsterConfig.OMPI_PREINSTALL_PROPERTY_KEY, true);
    cli.conf.set(HamsterConfig.OMPI_HOME_PROPERTY_KEY, HAMSTER_HOME);

    // create a add file/archive
    createLocalFile("file.data", 1111);
    createLocalFile("archive.tar.gz", 2222);

    cli.processCli("-s file.data --preload-archives archive.tar.gz -x PATH=/user/defined/path -x ENV0=env0 -np 2 hello"
        .split(" "));

    // set this job state to finished
    MockClientRMProtocol mockYarnClient = (MockClientRMProtocol) cli.client;
    mockYarnClient.setYarnApplicationState(YarnApplicationState.FINISHED);
    mockYarnClient.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

    // get new application
    cli.getNewApplication();

    // submit
    ApplicationSubmissionContext ctx = cli.submitApplication();

    // check if file existed in local resources
    assertFileExists(ctx, fs, "file.data", 1111, LocalResourceType.FILE);
    assertFileExists(ctx, fs, "archive", 2222, LocalResourceType.ARCHIVE);

    // check if env exists (default added envs)
    assertEnvExists(ctx, "LD_LIBRARY_PATH", HAMSTER_HOME + "/lib");

    // user specified envs
    assertEnvExists(ctx, "PATH", "/user/defined/path");
    assertEnvExists(ctx, "ENV0", "env0");

    // check command line
    Assert.assertEquals(ctx.getAMContainerSpec().getCommands().get(0),
        "$JAVA_HOME/bin/java -Xmx512M -Xms16M -cp "
            + "./*:$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:"
            + "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:"
            + "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:"
            + "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:"
            + "$HADOOP_YARN_HOME/share/hadoop/yarn/*:"
            + "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:hamster-core.jar "
            + "com.pivotal.hamster.appmaster.HamsterAppMaster "
            + "mpirun -mca ras yarn -mca plm yarn -mca state yarn -mca odls "
            + "yarn --np 2 hello 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr");
  }

  private void assertFileExists(ApplicationSubmissionContext ctx,
      MockFileSystem fs, String resourceKey, int size, LocalResourceType type) {
    Map<String, LocalResource> resources = ctx.getAMContainerSpec()
        .getLocalResources();
    for (Entry<String, LocalResource> entry : resources.entrySet()) {
      if (StringUtils.equals(entry.getKey(), resourceKey)) {
        LocalResource val = entry.getValue();
        Assert.assertEquals(type, val.getType());
        Assert.assertEquals(size, val.getSize());
      }
    }
  }

  private void assertEnvExists(ApplicationSubmissionContext ctx, String envKey,
      String envValue) {
    Map<String, String> envs = ctx.getAMContainerSpec().getEnvironment();
    boolean keyContains = false;
    for (Entry<String, String> entry : envs.entrySet()) {
      if (StringUtils.equals(entry.getKey(), envKey)) {
        keyContains = true;
        boolean valueContains = false;
        for (String value : entry.getValue().split(":")) {
          if (StringUtils.equals(envValue, value)) {
            valueContains = true;
          }
        }
        Assert.assertTrue(valueContains);
        break;
      }
    }
    Assert.assertTrue(keyContains);
  }

  private void createLocalFile(String path, int len) throws IOException {
    File f = new File(path);
    f.createNewFile();
    f.deleteOnExit();
    FileOutputStream os = new FileOutputStream(f);
    os.write(new byte[len]);
  }
}