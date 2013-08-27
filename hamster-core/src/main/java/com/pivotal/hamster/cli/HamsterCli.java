package com.pivotal.hamster.cli;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.pivotal.hamster.cli.parser.CliParser;
import com.pivotal.hamster.cli.processor.CliProcessor;
import com.pivotal.hamster.cli.processor.DebugProcessor;
import com.pivotal.hamster.cli.processor.EnvProcessor;
import com.pivotal.hamster.cli.processor.ExecutableProcessor;
import com.pivotal.hamster.cli.processor.HamsterCliProcessor;
import com.pivotal.hamster.cli.processor.HelpProcessor;
import com.pivotal.hamster.cli.processor.HostProcessor;
import com.pivotal.hamster.cli.processor.NotApplicableOptionsProcessor;
import com.pivotal.hamster.cli.processor.NotSupportedOptionsProcessor;
import com.pivotal.hamster.cli.processor.NpProcessor;
import com.pivotal.hamster.cli.processor.PathProcessor;
import com.pivotal.hamster.cli.processor.PrefixProcessor;
import com.pivotal.hamster.cli.processor.PreloadFilesProcessor;
import com.pivotal.hamster.cli.processor.VerboseProcessor;
import com.pivotal.hamster.cli.processor.VersionProcessor;
import com.pivotal.hamster.cli.processor.CliProcessor.ProcessResultType;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.CommandLine;
import com.pivotal.hamster.commons.cli.HelpFormatter;
import com.pivotal.hamster.commons.cli.Option;
import com.pivotal.hamster.commons.cli.ParseException;
import com.pivotal.hamster.yarnexecutor.YarnExecutor;

public class HamsterCli {
  private static final Log LOG = LogFactory.getLog(HamsterCli.class);
  private final String DEFAULT_HAMSTER_DIR_IN_HDFS = "hamster";
  
  List<CliProcessor> cliProcessors;
  AppLaunchContext context;
  ApplicationId appId;
  Configuration conf;
  ClientRMProtocol client;
  List<Option> options;
  RecordFactory recordFactory;
  boolean initialized;
  
  public HamsterCli() {
    context = new AppLaunchContext();
    this.conf = new YarnConfiguration();
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
    initialized = false;
  }
  
  void initialize() {
    cliProcessors = new ArrayList<CliProcessor>();
    
    // add cliProcessors
    cliProcessors.add(new HelpProcessor());
    cliProcessors.add(new VersionProcessor());
    cliProcessors.add(new VerboseProcessor());
    cliProcessors.add(new DebugProcessor());
    cliProcessors.add(new NotSupportedOptionsProcessor());
    cliProcessors.add(new NotApplicableOptionsProcessor());
    cliProcessors.add(new NpProcessor());
    cliProcessors.add(new HostProcessor());
    cliProcessors.add(new PrefixProcessor());
    cliProcessors.add(new PathProcessor());
    cliProcessors.add(new PreloadFilesProcessor());
    cliProcessors.add(new EnvProcessor());
    cliProcessors.add(new HamsterCliProcessor());
    cliProcessors.add(new ExecutableProcessor());
    
    conf.addResource("hamster-site.xml");
    createYarnRPC();
    
    // read ompi-home
    String defaultPrefix = conf.get(HamsterConfig.OMPI_HOME_PROPERTY_KEY);
    context.setPrefix(defaultPrefix);
  }
  
  /**
   * Do cli parsing and checking
   * @param args
   * @return continue running? (may not running if meet options like "--help") 
   * @throws ParseException
   */
  public boolean processCli(String[] args) throws ParseException {
    // prevent re-call this function
    if (initialized) {
      return true;
    }
    
    initialize();
    
    // init CliParser do the first parsing
    CliParser parser = new CliParser("1.7");
    CommandLine cli = parser.parse(args);
    options = new ArrayList<Option>();
    for (Option op : cli.getOptions()) {
      options.add(op);
    }
    context.setArgs(cli.getArgs());
    
    ProcessResultType resultType = ProcessResultType.SUCCEED;
    for (CliProcessor processor : cliProcessors) {
      resultType = processor.process(options, context);
      
      // if show help
      if (ProcessResultType.HELP_TERMINATED == resultType) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setDescPadding(formatter.getWidth());
        formatter.printHelp("hamster", parser.getOptions());
        break;
      } else if (ProcessResultType.VERSION_TERMINATED == resultType) {
        System.out.println("Hamster version: 0.8");
        break;
      }
    }
    
    if (resultType != ProcessResultType.SUCCEED) {
      return false;
    }
    
    return true;
  }
  
  public void doSubmit() throws YarnRemoteException, IOException, HamsterException, InterruptedException {
    getNewApplication();
    
    submitApplication();
  }
  
  void setContainerCtxResource(ContainerLaunchContext ctx) {
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    
    // we will use user specified memory to mpirun, by default, we will use 1024M
    resource.setMemory(HamsterConfig.DEFAULT_HAMSTER_MEM);
    
    // set virtual cores, by default we will use 1 core
    resource.setVirtualCores(HamsterConfig.DEFAULT_HAMSTER_CPU);
    
    ctx.setResource(resource);
  }
  
  FileSystem getRemoteFileSystem() throws IOException {
    return FileSystem.get(conf);
  }
  
  LocalResource constructLocalResource(FileSystem fs, String dirInHDFS,
      String filenameInHDFS, LocalResourceType type,
      LocalResourceVisibility visibility) throws IOException {
    LocalResource res = recordFactory.newRecordInstance(LocalResource.class);
    Path path = new Path(dirInHDFS, filenameInHDFS);
    FileStatus fsStatus = fs.getFileStatus(path);
    res.setResource(ConverterUtils.getYarnUrlFromPath(fsStatus.getPath()));
    res.setSize(fsStatus.getLen());
    res.setTimestamp(fsStatus.getModificationTime());
    res.setType(type);
    res.setVisibility(visibility);
    return res;
  }
  
  LocalResource constructLocalResource(FileSystem fs, String dirInHDFS,
      String fileNameInHDFS, LocalResourceType type) throws IOException {
    return constructLocalResource(fs, dirInHDFS, fileNameInHDFS, type,
        LocalResourceVisibility.APPLICATION);
  }
  
  String uploadFileToHDFS(String fullPath, FileSystem fs, String dirInHDFS) throws IOException {
    //parse fullPath to obtain localFilePath and fileName
    String localFilePath = null;
    String fileName = null;
    if (-1 != fullPath.indexOf('#')) {
      String[] splitPath = fullPath.split("#");
      localFilePath = splitPath[0];
      fileName = splitPath[1];
    } else {
      localFilePath = fullPath;
      File f = new File(localFilePath);
      fileName = f.getName();
    }
    
    //upload local file to HDFS
    Path filePathInHDFS = new Path(dirInHDFS, fileName);
    if (context.getVerbose()) {
      LOG.info("upload file:" + localFilePath + " to path:" + filePathInHDFS);
    }
    FSDataOutputStream os = fs.create(filePathInHDFS);
    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localFilePath));
    byte[] buffer = new byte[1024];
    int len = 0;
    while (-1 != (len = bis.read(buffer))) {
      os.write(buffer, 0, len);
    }
    os.flush();
    os.close();
    bis.close();
    return fileName;
  }
  
  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
  
  void processHamsterJar(Map<String, LocalResource> localResources, FileSystem fs, Path publicUploadPath) throws IOException {
    // upload our jar to staging area
    boolean needUpload = true;
    String jarPath = findContainingJar(YarnExecutor.class);

    if (jarPath == null) {
      LOG.error("cannot find any jar contains YarnExecutor");
      throw new IOException("cannot find any jar contains YarnExecutor");
    }
    File jarFile = new File(jarPath);
    Path uploadFilePath = new Path(publicUploadPath, jarFile.getName());

    if (fs.exists(uploadFilePath)) {
      if (!fs.isFile(uploadFilePath)) {
        LOG.error("error when trying to upload a file, but a same name directory already existed in target path:" + uploadFilePath.toUri().toString());
        throw new IOException("error when trying to upload a file, but a same name directory already existed in target path:" + uploadFilePath.toUri().toString());
      }
      FileStatus fsStatus = fs.getFileStatus(uploadFilePath);
      if (fsStatus.getLen() == jarFile.length()) {
        needUpload = false;
      } else {
        fs.delete(uploadFilePath, false);
      }
    }
    
    // will upload this to staging area
    if (needUpload) {
      uploadFileToHDFS(jarFile.getAbsolutePath(), fs, publicUploadPath.toString());
    }
    
    // add file info to localResources
    LocalResource res = this.constructLocalResource(fs, publicUploadPath.toString(),
        jarFile.getName(), LocalResourceType.FILE,
        LocalResourceVisibility.PUBLIC);
    localResources.put("hamster-core.jar", res);
  }
  
  /**
   * user specified files/archives may not only needed by mpirun, but also needed by launched processes,
   * so we will searilize resources map to a file, upload it to staging area, and it will be read by AM,
   * AM will deserialize it and put add it to launche context when launch user specified processes
   * @throws IOException 
   */
  void serializeLocalResourceToFile(Map<String, LocalResource> resources, FileSystem fs, Path appUploadPath) throws IOException {
    /*
     * file format,
     * 4 bytes (n), number of resources
     * <following is n parts of key/value
     * 4 bytes (key-len), length of key, followed by key-len bytes is content of key (java style, need add \0)
     * 4 bytes (value-len), length of value, following by value-len bytes is serialized pb LocalResource
     */
    String filename = HamsterConfig.DEFAULT_LOCALRESOURCE_SERIALIZED_FILENAME + "." + System.currentTimeMillis();
    File file = new File(filename);
    file.deleteOnExit();
    
    if (!file.createNewFile()) {
      LOG.error("create file for local-resource pb serialize failed, filename:" + filename);
      throw new IOException("create file for local-resource pb serialize failed, filename:" + filename);
    }
    
    DataOutputStream os = new DataOutputStream(new FileOutputStream(file));
    
    // write n;
    os.writeInt(resources.size());
    
    for (Entry<String, LocalResource> e : resources.entrySet()) {
      String key = e.getKey();
      // write key-len
      os.writeInt(key.length());
      
      // write key-content
      os.write(key.getBytes());
      
      LocalResourcePBImpl value = (LocalResourcePBImpl)e.getValue();
      
      // write value-len
      os.writeInt(value.getProto().getSerializedSize());
      
      // write value-content
      os.write(value.getProto().toByteArray());
    }
    
    os.close();
    
    // upload it to staging-are
    String fileNameInStagingArea = uploadFileToHDFS(filename, fs, appUploadPath.toString());
    LocalResource res = constructLocalResource(fs, appUploadPath.toString(), fileNameInStagingArea, LocalResourceType.FILE);
    
    // add this to local-resource, but this file doesn't need to be added to serialized file
    resources.put(HamsterConfig.DEFAULT_LOCALRESOURCE_SERIALIZED_FILENAME, res);
    
    file.delete();
  }
  
  void dumpParamtersToConf() throws IOException {
    String hostList = null;
    boolean isUserDefinedPolicy = false;
    
    // try to dump host list to conf, if the host list is directly set, we will not try to expand host expr
    if (context.getHosts() != null) {
      hostList = context.getHosts();
    }
    if (null != hostList) {
      conf.set(HamsterConfig.USER_POLICY_HOST_LIST_KEY, hostList);
      isUserDefinedPolicy = true;
    }
    
    // try to dump max-ppn
    if (context.getMaxPpn() > 0 && context.getMaxPpn() != Integer.MAX_VALUE) {
      conf.setInt(HamsterConfig.USER_POLICY_MPROC_KEY, context.getMaxPpn());
      isUserDefinedPolicy = true;
    }
    
    // set allocation strategy used by AM
    if (isUserDefinedPolicy) {
      // when user-policy specified (like host/max-ppn, etc.), 
      // we will ignore allocation strategy in hamster-site.xml
      conf.set(HamsterConfig.ALLOCATION_STRATEGY_KEY, HamsterConfig.USER_POLICY_DRIVEN_ALLOCATION_STRATEGY);
    } else {
      if (StringUtils.equalsIgnoreCase(context.getPolicy(), "compute-locality")
          || StringUtils.equalsIgnoreCase(context.getPolicy(), "cl")) {
        conf.set(HamsterConfig.ALLOCATION_STRATEGY_KEY, HamsterConfig.PROBABILITY_BASED_ALLOCATION_STRATEGY);
      } else {
        conf.set(HamsterConfig.ALLOCATION_STRATEGY_KEY, HamsterConfig.DEFAULT_HAMSTER_ALLOCATION_STRATEGY);
      }
    }
    
    // set allocation timeout
    if (context.getMaxAt() > 0) {
      conf.setInt(HamsterConfig.ALLOCATION_TIMEOUT_KEY, context.getMaxAt());
    }
  }
  
  /**
   * user specified (or set by us) configuration will be needed by AM as well,
   * so we need serialize it and upload it to staging area
   */
  void serializeLocalConfToFile(Map<String, LocalResource> resources, FileSystem fs, Path appUploadPath) throws IOException {
    // first we need add necessary parameters to configuration
    dumpParamtersToConf();
    
    String filename = HamsterConfig.DEFAULT_LOCALCONF_SERIALIZED_FILENAME + "." + System.currentTimeMillis();
    File file = new File(filename);
    file.deleteOnExit();
    
    if (!file.createNewFile()) {
      LOG.error("create file for local-conf serialize failed, filename:" + filename);
      throw new IOException("create file for local-conf serialize failed, filename:" + filename);
    }
    
    // serialize it to local
    DataOutputStream os = new DataOutputStream(new FileOutputStream(file));
    conf.write(os);
    os.close();
    
    // upload it to staging area
    String filenameInStagingArea = uploadFileToHDFS(filename, fs, appUploadPath.toString());
    LocalResource res = constructLocalResource(fs, appUploadPath.toString(), filenameInStagingArea, LocalResourceType.FILE);
    
    // add this to local-resource
    resources.put(HamsterConfig.DEFAULT_LOCALCONF_SERIALIZED_FILENAME, res);
    
    file.delete();
  }
  
  void setContainerCtxLocalResources(ContainerLaunchContext ctx) throws IOException {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    
    // if the dirInHDFS does not exists, we will create it on HDFS
    String uploadRootPath = this.conf.get("com.pivotal.hamster.hdfs.dir", DEFAULT_HAMSTER_DIR_IN_HDFS);
    FileSystem fs = getRemoteFileSystem();
    
    // upload path for this app
    Path appUploadPath = new Path(uploadRootPath, "app_upload_" + appId.getClusterTimestamp() + "_" + appId.getId());
    
    // upload path for public resources (like OMPI binary)
    Path publicUploadPath = new Path(uploadRootPath, "public"); 
    
    // normalize app upload path if it's not absolute path
    if (!appUploadPath.isAbsolute()) {
      appUploadPath = new Path(fs.getHomeDirectory().toString() + "/" + appUploadPath.toString());
    }
    
    // normalize public upload path if it's not absolute path
    if (!publicUploadPath.isAbsolute()) {
      publicUploadPath = new Path(fs.getHomeDirectory().toString() + "/" + publicUploadPath.toString());
    }
    
    if (!fs.exists(appUploadPath)) {
      fs.mkdirs(appUploadPath);
    }
    
    if (!fs.exists(publicUploadPath)) {
      fs.mkdirs(publicUploadPath);
    }
    
    if (context.getVerbose()) {
      LOG.info("will upload app files to folder:" + appUploadPath.toUri().toString());
      LOG.info("will upload public files to folder:" + publicUploadPath.toUri().toString());
    }
    
    List<String> fileList = context.getFiles();
    List<String> archiveList = context.getArchives();
        
    //upload a placeholder
    if (0 == fileList.size() && 0 == archiveList.size()) {
      String placeHolderName = "Hamster_place_holder";
      Path filePathInHDFS = new Path(appUploadPath, placeHolderName);
      if (!fs.exists(filePathInHDFS)) {
        FSDataOutputStream os = fs.create(filePathInHDFS);
        os.write(new byte[1024]);
        os.flush();
        os.close();
      }
      LocalResource res = constructLocalResource(fs, appUploadPath.toString(), placeHolderName, LocalResourceType.FILE);
      localResources.put(placeHolderName, res);
    }
    
    //obtain file from fileList one by one, and then upload it to HDFS 
    Iterator<String> fileIt = fileList.iterator();
    while (fileIt.hasNext()) {
      String fullPath = fileIt.next();
      String fileNameInHDFS = uploadFileToHDFS(fullPath, fs, appUploadPath.toString());
      LocalResource res = constructLocalResource(fs, appUploadPath.toString(), fileNameInHDFS, LocalResourceType.FILE);
      localResources.put(fileNameInHDFS, res);
    }
    
    //obtain archive from archiveList one by one, and then upload it to HDFS
    Iterator<String> archIt = archiveList.iterator();
    while (archIt.hasNext()) {
      String fullPath = archIt.next();
      String archiveNameInHDFS = uploadFileToHDFS(fullPath, fs, appUploadPath.toString());
      LocalResource res = constructLocalResource(fs, appUploadPath.toString(), archiveNameInHDFS, LocalResourceType.ARCHIVE);
      //remove postfix from archive file name as the key
      String key = null;
      if (archiveNameInHDFS.endsWith(".tar.gz")) {
        key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tar.gz"));
      } else if (archiveNameInHDFS.endsWith(".zip")) {
        key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".zip"));
      } else if (archiveNameInHDFS.endsWith(".tar")) {
        key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tar"));
      } else if (archiveNameInHDFS.endsWith(".tgz")) {
        key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tgz"));
      } else {
        LOG.warn("the archive file not with ordinary name, use the whole file name as key:" + fullPath);
        key = new File(fullPath).getName();
      }
      localResources.put(key, res);
    }
    
    // check if we need to upload hamster jar and add it to local resources
    processHamsterJar(localResources, fs, publicUploadPath);
    
    // serialize local resources for AM
    serializeLocalResourceToFile(localResources, fs, appUploadPath);
    
    // serialize conf for AM
    serializeLocalConfToFile(localResources, fs, appUploadPath);
    
    ctx.setLocalResources(localResources);
  }
  
  void setContainerCtxCommand(ContainerLaunchContext ctx) {
    String command = getUserCli();
    List<String> cmds = new ArrayList<String>();
    
    if (context.getVerbose()) {
      LOG.info("YARN AM's command line:" + command);
    }
    
    // set it to 
    cmds.add(command);
    ctx.setCommands(cmds);
  }
  
  ApplicationSubmissionContext createAppSubmissionCtx() throws IOException {
    // get application submission context
    ApplicationSubmissionContext val = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    // get container launch context (for command, local resource, etc.)
    ContainerLaunchContext ctx = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    
    // set app-id to app context
    val.setApplicationId(appId);

    // set resource spec to container launch context
    setContainerCtxResource(ctx);
    
    // set local resource 
    setContainerCtxLocalResources(ctx);

    // set env to ctx
    setContainerCtxEnvs(ctx);
    
    // set command for container launch context
    setContainerCtxCommand(ctx);
    
    // set application name
    val.setApplicationName("hamster");
    
    // set container launch context to app context
    val.setAMContainerSpec(ctx);
   
    return val;
  }
  
  String getUserCli() {
    String[] args = getUserParam();
    return convertArgsToCmd(args);
  }
  
  static String convertArgsToCmd(String[] args) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < args.length - 1; i++) {
      sb.append(args[i]);
      sb.append(" ");
    }
    sb.append(args[args.length - 1]);
    return sb.toString();
  }
  
  String[] getUserParam() {
    List<String> userParams = new ArrayList<String>();
    
    /*
     * we need specify commandline options for running AM first
     */
    userParams.add("$JAVA_HOME/bin/java");
    
    /*
     * we need to specify JVM param for running AM, because we will do a "fork"
     * in AM, so we have to use at most 1/2 mem of total, but luckily, it should
     * be enough for us. :)
     */
    int xmx = 512;
    int xms = 16;
    userParams.add(String.format("-Xmx%dM -Xms%dM", xmx, xms));
    
    /*
     * specify AM's jar and class
     */
    userParams.add("-cp");
    userParams.add(context.getEnvs().get("CLASSPATH"));
    userParams.add("com.pivotal.hamster.appmaster.HamsterAppMaster");
    
    /*
     * check if we need do valgrind check
     */
    if (context.getValgrind()) {
      userParams.add("valgrind");
      userParams.add("--tool=memcheck");
      userParams.add("--track-origins=yes");
      userParams.add("--leak-check=full");
    }
    
    /*
     * start specify mpirun parameters
     */
    userParams.add("mpirun");
    
    /*
     * hard code to choose ras, plm, odls, state modules 
     */
    userParams.add("-mca"); userParams.add("ras"); userParams.add("yarn");
    userParams.add("-mca"); userParams.add("plm"); userParams.add("yarn");
    userParams.add("-mca"); userParams.add("state"); userParams.add("yarn");
    userParams.add("-mca"); userParams.add("odls"); userParams.add("yarn");

    /*
     * output other parameters
     */
    for (Option op : options) {
      if (StringUtils.equals(op.getOpt(), "mca") || StringUtils.equals(op.getOpt(), "gmca")) {
        userParams.add("-" + op.getOpt());
        userParams.add(op.getValues()[0]);
        userParams.add(op.getValues()[1]);
      } else {
        if (op.hasLongOpt()) {
          userParams.add("--" + op.getLongOpt());
        } else {
          userParams.add("-" + op.getOpt());
        }
        if (op.hasArg()) {
          userParams.add(op.getValue());
        }
      }
    }
    
    /*
     * directly use user's args
     */
    for (String op : context.getArgs()) {
      userParams.add(op);
    }
    
    /*
     * append log output
     */
    // append log output
    userParams.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    userParams.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
    
    return userParams.toArray(new String[0]);
  }
  
  void setHamsterDebugEnvs() {
    // add default debug env
    context.setEnv("OMPI_MCA_state_base_verbose", "5");
    context.setEnv("OMPI_MCA_plm_base_verbose", "5");
    context.setEnv("OMPI_MCA_rmaps_base_verbose", "5");
    context.setEnv("OMPI_MCA_dfs_base_verbose", "5");
    context.setEnv("OMPI_MCA_ess_base_verbose", "5");
    context.setEnv("OMPI_MCA_routed_base_verbose", "5");
    context.setEnv("OMPI_MCA_odls_base_verbose", "5");
    context.setEnv("OMPI_MCA_ras_base_verbose", "5");
    context.setEnv("OMPI_MCA_rml_base_verbose", "5");
    context.setEnv("OMPI_MCA_grpcomm_base_verbose", "5");
    context.setEnv("OMPI_MCA_rmaps_base_display_map", "1");
    context.setEnv("OMPI_MCA_errmgr_base_verbose", "5");
    context.setEnv("OMPI_MCA_nidmap_verbose", "10");
  }
  
  void setHamsterModuleMcaEnvs() {
    context.setEnv("OMPI_MCA_plm", "yarn");
    context.setEnv("OMPI_MCA_ras", "yarn");
    context.setEnv("OMPI_MCA_odls", "yarn");
    context.setEnv("OMPI_MCA_state", "yarn");
  }
  
  void addClasspathToEnv() {
    context.appendEnv("CLASSPATH", "./*");
    for (String cp : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) {
      context.appendEnv("CLASSPATH", cp);
    }
    context.appendEnv("CLASSPATH", "hamster-core.jar");
  }
  
  void setContainerCtxEnvs(ContainerLaunchContext ctx) throws IOException {
    context.appendEnv("PATH", "./");
    if (System.getenv("PATH") != null) {
      context.appendEnv("PATH", System.getenv("PATH"));
    }
    context.appendEnv("PATH", "$PATH");
    context.appendEnv("LD_LIBRARY_PATH", "./");
    if (System.getenv("LD_LIBRARY_PATH") != null) {
      context.appendEnv("LD_LIBRARY_PATH",  System.getenv("LD_LIBRARY_PATH"));
    }
    context.appendEnv("LD_LIBRARY_PATH", "$LD_LIBRARY_PATH");
    context.appendEnv("DYLD_LIBRARY_PATH", "./");
    if (System.getenv("DYLD_LIBRARY_PATH") != null) {
      context.appendEnv("DYLD_LIBRARY_PATH", System.getenv("DYLD_LIBRARY_PATH"));
    }
    context.appendEnv("DYLD_LIBRARY_PATH", "DYLD_LIBRARY_PATH");
    
    // add debug envs
    if (context.getVerbose() || context.getDebug()) {
      setHamsterDebugEnvs();
    }
    
    // add mca modules for selects
    setHamsterModuleMcaEnvs();
    
    // add java home
    context.setEnvIfAbsent("JAVA_HOME", System.getenv("JAVA_HOME") !=null ? System.getenv("JAVA_HOME") : "");
    
    // set CPU/mem key
    int mem = context.getMem();
    context.setEnv(HamsterConfig.HAMSTER_MEM_ENV_KEY, String.valueOf(mem));
    int cpu = context.getCpu();
    context.setEnv(HamsterConfig.HAMSTER_CPU_ENV_KEY, String.valueOf(cpu));
    
    // set pb file env 
    context.setEnv(HamsterConfig.YARN_PB_FILE_ENV_KEY, context.getPrefix() + "/etc/protos/hamster_protos.pb");
    
    // set CLASSPATH
    addClasspathToEnv();
    
    if (context.getVerbose()) {
      context.setEnv("HAMSTER_VERBOSE", "yes");
    }
    
    if (context.getVerbose()) {
      // print env out
      LOG.info("all set envs");
      for (Entry<String, String> e : context.getEnvs().entrySet()) {
        LOG.info("{" + e.getKey() + "=" + e.getValue() + "}");
      }
    }
    
    ctx.setEnvironment(context.getEnvs());
  }
  
  void createYarnRPC() {
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    this.client = (ClientRMProtocol) (rpc.getProxy(ClientRMProtocol.class,
        rmAddress, conf));
  }
  
  void getNewApplication() throws IOException, YarnRemoteException {
    if (client == null) {
      throw new IOException("should initialize YARN client first.");
    }
    GetNewApplicationRequest request = recordFactory
        .newRecordInstance(GetNewApplicationRequest.class);
    GetNewApplicationResponse newAppResponse = client.getNewApplication(request);
    appId = newAppResponse.getApplicationId();
  }
  
  FinalApplicationStatus waitForApplicationTerminated() throws IOException, InterruptedException {
    // query request
    GetApplicationReportRequest reportRequest;
    ApplicationReport report;
    YarnApplicationState state;
    YarnApplicationState preState = YarnApplicationState.NEW;
    String trackingUrl = null;
    
    reportRequest = recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(appId);
    
    // poll RM, get AM state
    report = client.getApplicationReport(reportRequest).getApplicationReport();
    state = report.getYarnApplicationState();
    while (true) {
      report = client.getApplicationReport(reportRequest).getApplicationReport();
      preState = state;
      if (report.getTrackingUrl() != null && (!report.getTrackingUrl().isEmpty())) {
        if (trackingUrl == null) {
          trackingUrl = report.getTrackingUrl();
          LOG.info("tracking URL is: http://" + trackingUrl);
        }
      }
      state = report.getYarnApplicationState();
      
      // state changed
      if (preState != state) {
        LOG.info("yarn application state transfered from [" + preState.name() + "] to [" + state.name() + "]");
      }
      
      // application terminated
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.FINISHED || state == YarnApplicationState.KILLED) {
        break;
      }
      Thread.sleep(100);
    }
    
    FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
    if (finalStatus != FinalApplicationStatus.SUCCEEDED) {
      LOG.error("Final state of AppMaster is," + finalStatus.name());
    } else {
      LOG.info("AppMaster is successfully finished.");
    }
    
    return finalStatus;
  }
  
  ApplicationSubmissionContext submitApplication() throws HamsterException,
      InterruptedException, IOException {
    if (client == null) {
      throw new HamsterException("should initialize YARN client first.");
    }

    // submit application
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext submissionCtx = createAppSubmissionCtx();
    submitRequest.setApplicationSubmissionContext(submissionCtx);
    client.submitApplication(submitRequest);

    // wait for application get started
    waitForApplicationTerminated();
    
    return submissionCtx;
  }

  public static void main(String[] args) throws HamsterException,
      YarnRemoteException, ParseException, IOException, InterruptedException {
    HamsterCli cli = new HamsterCli();
    if (cli.processCli(args)) {
      cli.doSubmit();
    }
  }
}
