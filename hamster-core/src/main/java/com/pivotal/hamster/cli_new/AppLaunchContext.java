package com.pivotal.hamster.cli_new;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppLaunchContext {
  /* comma splitted hosts */
  String hosts;
  List<String> files;
  List<String> archives;
  Map<String, String> envs;
  boolean verbose;
  int cpu;
  int mem;
  boolean valgrind;
  int maxPpn;
  int maxAt;
  String[] args;
  String prefix;
  boolean debug;
  
  public AppLaunchContext() {
    hosts = null;
    files = new ArrayList<String>();
    archives = new ArrayList<String>();
    envs = new HashMap<String, String>();
    verbose = false;
    cpu = 1;
    mem = 1024;
    valgrind = false;
    maxPpn = Integer.MAX_VALUE;
    maxAt = Integer.MAX_VALUE;
    args = null;
    prefix = null;
    debug = false;
  }
  
  public void setDebug(boolean debug) {
    this.debug = debug;
  }
  
  public boolean getDebug() {
    return this.debug;
  }
  
  public void setHosts(String hosts) {
    this.hosts = hosts;
  }
  
  public String getHosts() {
    return hosts;
  }
  
  public void setArgs(String[] args) {
    this.args = args;
  }
  
  public String[] getArgs() {
    return args;
  }
  
  public void addCommaSplitFiles(String commaSplitFiles) {
    for (String file : commaSplitFiles.split(",")) {
      file = file.trim();
      if (file.isEmpty()) {
        continue;
      }
      files.add(file);
    }
  }
  
  public void addFile(String file) {
    files.add(file);
  }
  
  public List<String> getFiles() {
    return files;
  }
  
  public void addCommaSplitArchives(String commaSplitFiles) {
    for (String file : commaSplitFiles.split(",")) {
      file = file.trim();
      if (file.isEmpty()) {
        continue;
      }
      archives.add(file);
    }
  }
  
  public void addArchive(String file) {
    archives.add(file);
  }
  
  public List<String> getArchives() {
    return archives;
  }
  
  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }
  
  public boolean getVerbose() {
    return this.verbose;
  }
  
  public void appendEnv(String key, String value) {
    if (!envs.containsKey(key)) {
      envs.put(key, value);
      return;
    }
    
    String oldValue = envs.get(key);
    if (oldValue.endsWith(":")) {
      value = oldValue + value;
    } else {
      value = oldValue + ":" + value;
    }
    envs.put(key, value);
  }
  
  public void setEnvIfAbsent(String key, String value) {
    if (envs.containsKey(key)) {
      return;
    }
    envs.put(key, value);
  }
  
  public void setEnv(String key, String value) {
    envs.put(key, value);
  }
  
  public Map<String, String> getEnvs() {
    return envs;
  }
  
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }
  
  public String getPrefix() {
    return prefix;
  }
  
  public void setCpu(int cpu) {
    this.cpu = cpu;
  }
  
  public int getCpu() {
    return cpu;
  }
  
  public void setMem(int mem) {
    this.mem = mem;
  }
  
  public int getMem() {
    return mem;
  }
  
  public void setValgrind(boolean valgrind) {
    this.valgrind = valgrind;
  }
  
  public boolean getValgrind() {
    return valgrind;
  }
  
  public void setMaxPpn(int maxPpn) {
    this.maxPpn = maxPpn;
  }
  
  public int getMaxPpn() {
    return maxPpn;
  }
  
  public void setMaxAt(int maxAt) {
    this.maxAt = maxAt;
  }
  
  public int getMaxAt() {
    return maxAt;
  }
}
