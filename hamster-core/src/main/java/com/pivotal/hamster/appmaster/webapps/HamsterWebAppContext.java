package com.pivotal.hamster.appmaster.webapps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.pivotal.hamster.common.LaunchContext;
import com.pivotal.hamster.common.ProcessName;
import com.pivotal.hamster.common.ProcessNameComparator;

/**
 * contexts will be used by web app
 */
public class HamsterWebAppContext {
  static SortedMap<ProcessName, LaunchContext> launchedProcesses = new TreeMap<ProcessName, LaunchContext>(new ProcessNameComparator());
  
  public synchronized static void addLaunchedProcess(LaunchContext ctx) {
    launchedProcesses.put(ctx.getName(), ctx);
  }
  
  public synchronized static LaunchContext[] getSortedLaunchContexts() {
    List<LaunchContext> ret = new ArrayList<LaunchContext>();
    for (Entry<ProcessName, LaunchContext> ctx : launchedProcesses.entrySet()) {
      ret.add(ctx.getValue().getCopy());
    }
    return ret.toArray(new LaunchContext[0]);
  }
}