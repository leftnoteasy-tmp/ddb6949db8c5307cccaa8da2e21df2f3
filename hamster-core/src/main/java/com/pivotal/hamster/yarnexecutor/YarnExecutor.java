package com.pivotal.hamster.yarnexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


public class YarnExecutor {
  static boolean errMark = false;
  
  static class StreamGobbler implements Runnable {
    BufferedReader reader;
    boolean out;

    public StreamGobbler(BufferedReader reader, boolean out) {
      this.reader = reader;
      this.out = out;
    }

    public void run() {
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
          if (out)
            System.out.println(line);
          else
            System.err.println(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
        errMark = true;
      }
    }
  }
  
  static String[] copyCurrentEnvs() {
    List<String> envs = new ArrayList<String>();
    for (Entry<String, String> entry : System.getenv().entrySet()) {
      envs.add(entry.getKey() + "=" + entry.getValue());
    }
    return envs.toArray(new String[0]);
  }
  
  String getApplicationIdFromPath() throws IOException {
    return new File(new File("../").getCanonicalPath()).getName();
  }
  
  String getUserNameFromPath() throws IOException {
    return new File(new File("../../../").getCanonicalPath()).getName();
  }
  
  File getPidRoot(String jobId) throws IOException {
    String localDirs = System.getenv("YARN_NM_LOCAL_DIRS");
    
    // if no such env set, just return
    if (null == localDirs || localDirs.isEmpty()) {
      return new File("../" + jobId);
    }
    
    // consist the rest parts of app and check which contains the jobId file
    String userName = getUserNameFromPath();
    String applicationId = getApplicationIdFromPath();
    String restPart = String.format("usercache/%s/appcache/%s/%s", userName, applicationId, jobId);
    for (String dir : localDirs.trim().split(",")) {
      String wholePath = dir + "/" + restPart;
      File pidRootFile = new File(wholePath);
      if (pidRootFile.exists() && pidRootFile.isDirectory()) {
        return pidRootFile;
      }
    }
    
    System.err.println("cannot find proper pidRoot, restPart:" + restPart + " dirs:" + localDirs);
    return null;
  }
  
  String createPidFile(String jobId, String vpId, boolean failed) throws IOException {
    // get pid root
    File pidRoot = getPidRoot(jobId);
    
    // see if dir created
    if ((null != pidRoot) && pidRoot.exists() && pidRoot.isDirectory()) {
      File pidFile;
      if (failed) {
        pidFile = new File(pidRoot, vpId + "_err");
      } else {
        pidFile = new File(pidRoot, vpId);
      }
      
      // clean pidFile if it exists
      if (pidFile.exists()) {
        pidFile.delete();
      }
      
      boolean flag = pidFile.createNewFile();
      if (!flag) {
        throw new IOException("create pidfile failed, path:" + pidFile.getAbsolutePath());
      }
      
      return pidFile.getAbsolutePath();
    } else {
      throw new IOException("get father directory for pid file failed, path:" + pidRoot);
    }
  }
  
  void run(String[] args) throws InterruptedException, IOException {
    // get jobid, vpid
    String jobId = args[0];
    String vpId = args[1];
    
    if (jobId == null || jobId.isEmpty() || vpId == null || vpId.isEmpty()) {
      System.err.println("jobid, vpid cannot be empty or null");
      throw new IOException("jobid, vpid cannot be empty or null");
    }
    
    // get real arguments need to be executed
    String[] execArgs = new String[args.length - 2];
    for (int i = 2; i < args.length; i++) {
      execArgs[i - 2] = args[i];
    }
    
    // try to launch process
    Process proc;
    try {
      proc = Runtime.getRuntime().exec(execArgs, copyCurrentEnvs());
    } catch (IOException e) {
      System.err.println("launch process [" + vpId + "] failed.");
      createPidFile(jobId, vpId, true);
      throw e;
    }
    
    // create pidFile
    String pidFile = createPidFile(jobId, vpId, false);
    
    // get err stream and out stream
    BufferedReader bre = new BufferedReader(new InputStreamReader(
        proc.getErrorStream()));
    BufferedReader bri = new BufferedReader(new InputStreamReader(
        proc.getInputStream()));

    // use thread fetch output
    Thread errThread = new Thread(new StreamGobbler(bre, false));
    Thread outThread = new Thread(new StreamGobbler(bri, true));
    
    errThread.start();
    outThread.start();
    
    // wait for thread die
    errThread.join();
    outThread.join();
    
    bri.close();
    bre.close();
    
    // get exit code and write it to pid file
    int exitCode = proc.waitFor();
    FileOutputStream os = new FileOutputStream(pidFile);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));
    writer.write(String.valueOf(exitCode));
    writer.close();
    
    // use same exit code 
    System.exit(exitCode);
  }
  
  /**
   * YarnExecutor <job-id> <vp-id> <exec-path> <args ...>
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    YarnExecutor exec = new YarnExecutor();
    exec.run(args);
  }
}
