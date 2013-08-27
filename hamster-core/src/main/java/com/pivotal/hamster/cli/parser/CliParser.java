package com.pivotal.hamster.cli.parser;

import com.pivotal.hamster.commons.cli.CommandLine;
import com.pivotal.hamster.commons.cli.CommandLineParser;
import com.pivotal.hamster.commons.cli.GnuParser;
import com.pivotal.hamster.commons.cli.Option;
import com.pivotal.hamster.commons.cli.Options;
import com.pivotal.hamster.commons.cli.ParseException;

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
    options.addOption(new Option("am", true, "Aggregate MCA parameter set file list")).
            addOption(new Option("app", "app", true, "Provide an appfile; ignore all other command line options")).
            addOption(new Option("bind-to", "bind-to", true, "Policy for binding processes [none (default) | hwthread | core | socket | numa | board] (supported qualifiers: overload-allowed,if-supported)")).
            addOption(new Option("bind-to-core", "bind-to-core", false, "Bind processes to cores")).
            addOption(new Option("bind-to-socket", "bind-to-socket", false, "Bind processes to sockets")).
            addOption(new Option("bynode", "bynode", false, "Whether to map and rank processes round-robin by node")).
            addOption(new Option("byslot", "byslot", false, "Whether to map and rank processes round-robin by slot")).
            addOption(new Option("c", true, "Number of processes to run")).
            addOption(new Option("np", "np", true, "Number of processes to run")).
            addOption(new Option("cf", "cartofile", true, "Provide a cartography file")).
            addOption(new Option("cpu-set", "cpu-set", true, "Comma-separated list of ranges specifying logical, cpus allocated to this job [default: none]")).
            addOption(new Option("d", false, "Enable debugging of OpenRTE")).
            addOption(new Option("debug-devel", "debug-devel", false, "Enable debugging of OpenRTE")).
            addOption(new Option("debug", "debug", false, "Invoke the user-level debugger indicated by the orte_base_user_debugger MCA parameter")).
            addOption(new Option("debug-daemons-file", "debug-daemons-file", false, "Enable debugging of any OpenRTE daemons used by this application, storing output in files")).
            addOption(new Option("debugger", "debugger", true, "Sequence of debuggers to search for when \"--debug\" is used")).
            addOption(new Option("default-hostfile", "default-hostfile", true, "Provide a default hostfile")).
            addOption(new Option("disable-recovery", "disable-recovery", false, "Disable recovery (resets all recovery options to off)")).
            addOption(new Option("display-allocaiton", "display-allocation", false, "Display the allocation being used by this job")).
            addOption(new Option("display-devel-allocation", "display-devel-allocation", false, "Display a detailed list (mostly intended for developers) of the allocation being used by this job")).
            addOption(new Option("display-devel-map", "display-devel-map", false, "Display a detailed process map (mostly intended for developers) just before launch")).
            addOption(new Option("display-diffable-map", "display-diffable-map", false, "Display a diffable process map (mostly intended for developers) just before launch")).
            addOption(new Option("display-map", "display-map", false, "Display the process map just before launch")).
            addOption(new Option("display-topo", "display-topo", false, "Display the topology as part of the process map (mostly intended for developers) just before launch")).
            addOption(new Option("do-not-launch", "do-not-launch", false, "Perform all necessary operations to prepare to launch the application, but do not actually launch it")).
            addOption(new Option("do-not-resolve", "do-not-resolve", false, "Do not attempt to resolve interfaces")).
            addOption(new Option("enable-recovery", "enable-recovery", false, "Enable recovery from process failure [Default = disabled]")).
            addOption(new Option("h", "help", false, "This help message")).
            addOption(new Option("H", true, "List of hosts to invoke processes on")).
            addOption(new Option("host", "host", true, "List of hosts to invoke processes on")).
            addOption(new Option("hetero-apps", "hetero-apps", false, "Indicates that multiple app_contexts are being provided that are a mix of 32/64 bit binaries")).
            addOption(new Option("hetero-nodes", "hetero-nodes", false, "Nodes in cluster may differ in topology, so send the topology back from each node [Default = false]")).
            addOption(new Option("hostfile", "hostfile", true, "Provide a hostfile")).
            addOption(new Option("launch-agent", "launch-agent", true, "Command used to start processes on remote nodes (default: orted)")).
            addOption(new Option("leave-session-attached", "leave-session-attached", false, "Enable debugging of OpenRTE")).
            addOption(new Option("machinefile", "machinefile", true, "Provide a hostfile")).
            addOption(new Option("map-by", "map-by", true, "Mapping Policy [slot (default) | hwthread | core | socket | numa | board | node]")).
            addOption(new Option("max-restarts", "max-restarts", true, "Max number of times to restart a failed process")).
            addOption(new Option("max-vm-size", "max-vm-size", true, "Number of processes to run")).
            addOption(new Option("N", true, "Launch n processes per node on all allocated nodes (synonym for npernode)")).
            addOption(new Option("n", "n", true, "Number of processes to run")).
            addOption(new Option("nolocal", "nolocal", false, "Do not run any MPI applications on the local node")).
            addOption(new Option("nooversubscribe", "nooversubscribe", false, "Nodes are not to be oversubscribed, even if the system supports such operation")).
            addOption(new Option("noprefix", "noprefix", false, "disable automatic --prefix behavior")).
            addOption(new Option("novm", "novm", false, "Execute without creating an allocation-spanning virtual machine (only start daemons on nodes hosting application procs)")).
            addOption(new Option("npernode", "npernode", true, "Launch n processes per node on all allocated nodes")).
            addOption(new Option("ompi-server", "ompi-server", true, "Specify the URI of the Open MPI server, or the name of the file (specified as file:filename) that contains that info")).
            addOption(new Option("output-filename", "output-filename", true, "Redirect output from application processes into filename.rank")).
            addOption(new Option("output-proctable", "output-proctable", false, "Output the debugger proctable after launch")).
            addOption(new Option("oversubscribe", "oversubscribe", false, "Nodes are allowed to be oversubscribed, even on a managed system")).
            addOption(new Option("path", "path", true, "PATH to be used to look for executables to start processes")).
            addOption(new Option("pernode", "pernode", false, "Launch one process per available node")).
            addOption(new Option("p", "policy", true, "policy for scheduling valid value : { default, compute-locality/cl")).
            addOption(new Option("ppr", "ppr", true, "Comma-separated list of number of processes on a given resource type [default: none]")).
            addOption(new Option("prefix", "prefix", true, "Prefix where Open MPI is installed on remote nodes")).
            addOption(new Option("preload-files", "preload-files", true, "Preload the comma separated list of files to the remote machines current working directory before starting the remote process.")).
            addOption(new Option("preload-archives", "preload-archives", true, "Preload the comma separated list of archives to the remote machines current working directory and un-zip before starting the remote process.")).
            addOption(new Option("preload-files-dest-dir", "preload-files-dest-dir", true, "The destination directory to use in conjunction with --preload-files. By default the absolute and relative paths provided by --preload-files are used.")).
            addOption(new Option("q", "quiet", false, "Suppress helpful messages")).
            addOption(new Option("rank-by", "rank-by", true, "Ranking Policy [slot (default) | hwthread | core | socket | numa | board | node]")).
            addOption(new Option("report-bindings", "report-bindings", false, "Whether to report process bindings to stderr")).
            addOption(new Option("report-child-jobs-separately", "report-child-jobs-separately", false, "Return the exit status of the primary job only")).
            addOption(new Option("report-events", "report-events", true, "Report events to a tool listening at the specified URI")).
            addOption(new Option("report-pid", "report-pid", true, "Printout pid on stdout [-], stderr [+], or a file [anything else]")).
            addOption(new Option("report-uri", "report-uri", true, "Printout URI on stdout [-], stderr [+], or a file [anything else]")).
            addOption(new Option("rf", "rankfile", true, "Provide a rankfile file")).
            addOption(new Option("s", "preload-binary", true, "Preload the binary on the remote machine before starting the remote process.")).
            addOption(new Option("server-wait-time", "server-wait-time", true, "Time in seconds to wait for ompi-server (default: 10 sec)")).
            addOption(new Option("show-progress", "show-progress", false, "Output a brief periodic report on launch progress")).
            addOption(new Option("slot-list", "slot-list", true, "List of processor IDs to bind processes to [default=NULL]")).
            addOption(new Option("stdin", "stdin", true, "Specify procs to receive stdin [rank, all, none] (default: 0, indicating rank 0)")).
            addOption(new Option("tag-output", "tag-output", false, "Tag all output with [job,rank]")).
            addOption(new Option("timestamp-output", "timestamp-output", false, "Timestamp all application process output")).
            addOption(new Option("tv", "tv", false, "Deprecated backwards compatibility flag; synonym for \"--debug\"")).
            addOption(new Option("use-hwthread-cpus", "use-hwthread-cpus", false, "Use hardware threads as independent cpus")).
            addOption(new Option("use-regexp", "use-regexp", false, "Use regular expressions for launch")).
            addOption(new Option("v", "verbose", false, "Be verbose")).
            addOption(new Option("V", "version", false, "Print version and exit")).
            addOption(new Option("wait-for-server", "wait-for-server", false, "If ompi-server is not already running, wait until it is detected (default: false)")).
            addOption(new Option("wd", "wd", true, "Synonym for --wdir")).
            addOption(new Option("wdir", "wdir", true, "Set the working directory of the started processes")).
            addOption(new Option("x", true, "Export an environment variable, optionally specifying a value (e.g., \"-x foo\" exports the environment variable foo and takes its value from the current environment; \"-x foo=bar\" exports the environment variable name foo and sets its value to \"bar\" in the started processes)")).
            addOption(new Option("xml", "xml", false, "Provide all output in XML format")).
            addOption(new Option("xml-file", "xml-file", true, "Provide all output in XML format to the specified file")).
            addOption(new Option("xterm", "xterm", true, "Create a new xterm window and display output from the specified ranks there"));
   
    // separatelly add mca option
    Option mcaOp = new Option("mca", "mca", true, "Pass context-specific MCA parameters; they are considered global if --gmca is not used and only one context is specified (arg0 is the parameter name; arg1 is the parameter value)");
    mcaOp.setArgs(2);
    options.addOption(mcaOp);
    
    // separatelly add gmca option
    Option gmcaOp = new Option("gmca", "mca", true, "Pass global MCA parameters that are applicable to all contexts (arg0 is the parameter name; arg1 is the parameter value)");
    mcaOp.setArgs(2);
    options.addOption(gmcaOp);
            
    // add options in hamster
    options.addOption(new Option("cpu", "cpu-per-proc", true, "specify how many v-cores allocated to each MPI proc")).
            addOption(new Option("mem", "mem-per-proc", true, "specify how many memory (in MB) allocated to each MPI proc")).
            addOption(new Option("hvalgrind", "hvalgrind", false, "debug option, using valgrind to check memory leaks")).
            addOption(new Option("max-proc", "max-proc", true, "maximum slots allocated for mpirun do mapping (should >= --np specified)")).
            addOption(new Option("max-node", "max-node", true, "maximum unique nodes count allocated")).
            addOption(new Option("min-proc", "min-proc", true, "minimum slots allocated for mpirun do mapping (should >= --np specified)")).
            addOption(new Option("max-ppn", "max-proc-per-node", true, "maximum proc number allocated in each node")).
            addOption(new Option("min-ppn", "min-proc-per-node", true, "minimum proc number allocated in each node")).
            addOption(new Option("max-at", "max-allocation-time", true, "maximum time used do allocation (in milli-seconds), after timeout, all allocated containers will be returned and job will be failed, we will use min{ $max-alloc, yarn.resourcemanager.rm.container-allocation.expiry-interval-ms} as the actual expired timeout"));
  }
}
