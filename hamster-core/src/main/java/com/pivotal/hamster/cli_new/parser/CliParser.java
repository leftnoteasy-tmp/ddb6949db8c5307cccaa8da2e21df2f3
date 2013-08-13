package com.pivotal.hamster.cli_new.parser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliParser {
  Options options;
  
  public CliParser(String version) {
    initialize();
  }
  
  public Options getOptions() {
    return options;
  }
  
  public CommandLine parse(String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    CommandLine commandline = parser.parse(options, args, true);
    return commandline;
  }
  
  void initialize() {
    options = new Options();

    // add options of original OMPI
    options.addOption(new Option("am", true, "")).
            addOption(new Option("app", "app", true, "")).
            addOption(new Option("bind-to", "bind-to", true, "")).
            addOption(new Option("bind-to-core", "bind-to-core", false, "")).
            addOption(new Option("bind-to-socket", "bind-to-socket", false, "")).
            addOption(new Option("bynode", "bynode", false, "")).
            addOption(new Option("byslot", "byslot", false, "")).
            addOption(new Option("c", true, "")).
            addOption(new Option("np", "np", true, "")).
            addOption(new Option("cf", "cartofile", true, "")).
            addOption(new Option("cpu-set", "cpu-set", true, "")).
            addOption(new Option("d", false, "")).
            addOption(new Option("debug-devel", "debug-devel", false, "")).
            addOption(new Option("debug", "debug", false, "")).
            addOption(new Option("debug-daemons-file", "debug-daemons-file", false, "")).
            addOption(new Option("debugger", "debugger", true, "")).
            addOption(new Option("default-hostfile", "default-hostfile", true, "")).
            addOption(new Option("disable-recovery", "disable-recovery", false, "")).
            addOption(new Option("display-allocaiton", "display-allocation", false, "")).
            addOption(new Option("display-devel-allocation", "display-devel-allocation", false, "")).
            addOption(new Option("display-devel-map", "display-devel-map", false, "")).
            addOption(new Option("display-diffable-map", "display-diffable-map", false, "")).
            addOption(new Option("display-map", "display-map", false, "")).
            addOption(new Option("display-topo", "display-topo", false, "")).
            addOption(new Option("do-not-launch", "do-not-launch", false, "")).
            addOption(new Option("do-not-resolve", "do-not-resolve", false, "")).
            addOption(new Option("enable-recovery", "enable-recovery", false, "")).
            addOption(new Option("h", "--help", false, "")).
            addOption(new Option("H", true, "")).
            addOption(new Option("host", "host", true, "")).
            addOption(new Option("hetero-apps", "hetero-apps", false, "")).
            addOption(new Option("hetero-nodes", "hetero-nodes", false, "")).
            addOption(new Option("hostfile", "hostfile", true, "")).
            addOption(new Option("launch-agent", "launch-agent", true, "")).
            addOption(new Option("leave-session-attached", "leave-session-attached", false, "")).
            addOption(new Option("machinefile", "machinefile", true, "")).
            addOption(new Option("map-by", "map-by", true, "")).
            addOption(new Option("max-restarts", "max-restarts", true, "")).
            addOption(new Option("max-vm-size", "max-vm-size", true, "")).
            addOption(new Option("N", true, "")).
            addOption(new Option("n", "n", true, "")).
            addOption(new Option("nolocal", "nolocal", false, "")).
            addOption(new Option("nooversubscribe", "nooversubscribe", false, "")).
            addOption(new Option("noprefix", "noprefix", false, "")).
            addOption(new Option("novm", "novm", false, "")).
            addOption(new Option("npernode", "npernode", true, "")).
            addOption(new Option("ompi-server", "ompi-server", true, "")).
            addOption(new Option("output-filename", "output-filename", true, "")).
            addOption(new Option("output-proctable", "output-proctable", false, "")).
            addOption(new Option("oversubscribe", "oversubscribe", false, "")).
            addOption(new Option("path", "path", true, "")).
            addOption(new Option("pernode", "pernode", false, "")).
            addOption(new Option("ppr", "ppr", true, "")).
            addOption(new Option("prefix", "prefix", true, "")).
            addOption(new Option("preload-files", "preload-files", true, "")).
            addOption(new Option("preload-archives", "preload-archives", true, "")).
            addOption(new Option("preload-files-dest-dir", "preload-files-dest-dir", true, "")).
            addOption(new Option("q", "quiet", false, "")).
            addOption(new Option("rank-by", "rank-by", true, "")).
            addOption(new Option("report-bindings", "report-bindings", false, "")).
            addOption(new Option("report-child-jobs-separately", "report-child-jobs-separately", false, "")).
            addOption(new Option("report-events", "report-events", true, "")).
            addOption(new Option("report-pid", "report-pid", true, "")).
            addOption(new Option("report-uri", "report-uri", true, "")).
            addOption(new Option("rf", "rankfile", true, "")).
            addOption(new Option("s", "preload-binary", false, "")).
            addOption(new Option("server-wait-time", "server-wait-time", true, "")).
            addOption(new Option("show-progress", "show-progress", false, "")).
            addOption(new Option("slot-list", "slot-list", true, "")).
            addOption(new Option("stdin", "stdin", true, "")).
            addOption(new Option("tag-output", "tag-output", false, "")).
            addOption(new Option("timestamp-output", "timestamp-output", false, "")).
            addOption(new Option("tv", "tv", false, "")).
            addOption(new Option("use-hwthread-cpus", "use-hwthread-cpus", false, "")).
            addOption(new Option("use-regexp", "use-regexp", false, "")).
            addOption(new Option("v", "verbose", false, "")).
            addOption(new Option("V", "version", false, "")).
            addOption(new Option("wait-for-server", "wait-for-server", false, "")).
            addOption(new Option("wd", "wd", true, "")).
            addOption(new Option("wdir", "wdir", true, "")).
            addOption(new Option("x", true, "")).
            addOption(new Option("xml", "xml", false, "")).
            addOption(new Option("xml-file", "xml-file", true, "")).
            addOption(new Option("xterm", "xterm", true, ""));
   
    // separatelly add mca option
    Option mcaOp = new Option("mca", "mca", true, "");
    mcaOp.setArgs(2);
    options.addOption(mcaOp);
    
    // separatelly add gmca option
    Option gmcaOp = new Option("gmca", "mca", true, "");
    mcaOp.setArgs(2);
    options.addOption(gmcaOp);
            
    // add options in hamster
    options.addOption(new Option("cpu", "cpu-per-proc", true, "")).
            addOption(new Option("mem", "mem-per-proc", true, "")).
            addOption(new Option("hvalgrind", "hvalgrind", false, "")).
            addOption(new Option("max-proc", "max-proc", true, "")).
            addOption(new Option("max-node", "max-node", true, "")).
            addOption(new Option("min-proc", "min-proc", true, "")).
            addOption(new Option("max-ppn", "max-proc-per-node", true, "")).
            addOption(new Option("min-ppn", "min-proc-per-node", true, "")).
            addOption(new Option("max-at", "max-allocation-time", true, ""));
  }
}
