package com.pivotal.hamster.cli.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.cli.parser.CliParserUTUtils;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;
import com.pivotal.hamster.commons.cli.ParseException;

public class HostProcessorTest {
  @Test
  public void testNoHostSpecified() throws ParseException {
    String[] input = "--help".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    processor.process(options, context);
    Assert.assertNull(context.getHosts());
  }
  
  @Test
  public void testHostSpecifiedOnly() throws ParseException {
    String[] input = "--host host1,host[2-3]".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    processor.process(options, context);
    assertHostsContains(context.getHosts(), new String[] { "host1", "host2", "host3" });
  }
  
  @Test
  public void testDefaultHostSpecifiedOnly() throws ParseException, IOException {
    writeFile("host1\nhost2\nhost3\n", "default_host_000.txt");
 
    String[] input = "--default-hostfile default_host_000.txt".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    processor.process(options, context);
    
    deleteFile("default_host_000.txt");
    
    assertHostsContains(context.getHosts(), new String[] { "host1", "host2", "host3" });
  }
  
  @Test
  public void testHostfileSpecifiedOnly() throws IOException, ParseException {
    writeFile("host1\nhost2\n#comment\n\nhost3\n", "host_000.txt");
    
    String[] input = "--hostfile host_000.txt".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    processor.process(options, context);
    
    deleteFile("host_000.txt");
    
    assertHostsContains(context.getHosts(), new String[] { "host1", "host2", "host3" });
  }
  
  @Test
  public void testTwoHostSpecified() throws ParseException {
    String[] input = "--host host1,host[2-3] --host host3,host4".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    boolean exception = false;
    try {
      processor.process(options, context);
    } catch (HamsterException e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }
  
  @Test
  public void testHostAndHostfileSpecifiedAtSameTime() throws ParseException {
    String[] input = "--host host1,host[2-3] --hostfile host.txt".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    boolean exception = false;
    try {
      processor.process(options, context);
    } catch (HamsterException e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }
  
  @Test
  public void testTwoHostfileSpecified() throws ParseException {
    String[] input = "--hostfile host1.txt --hostfile host.txt".split(" ");
    CliParserUTUtils.parseCli(input);
    List<Option> options = CliParserUTUtils.getOptions();
    AppLaunchContext context = CliParserUTUtils.getContext();
    HostProcessor processor = new HostProcessor();
    boolean exception = false;
    try {
      processor.process(options, context);
    } catch (HamsterException e) {
      exception = true;
    }
    Assert.assertTrue(exception);
  }
  
  private void assertHostsContains(String hosts, String[] checkHost) {
    if ((hosts == null || hosts.isEmpty()) && ((checkHost != null) && checkHost.length > 0)) {
      Assert.fail("hosts is empty or null");
    }
    Set<String> hostSet = new HashSet<String>();
    for (String h : hosts.trim().split(",")) {
      if (h.isEmpty()) {
        continue;
      }
      hostSet.add(h);
    }
    for (String c : checkHost) {
      if (!hostSet.contains(c)) {
        Assert.fail("host:" + c + " not contained");
      }
    }
    Assert.assertEquals(hostSet.size(), checkHost.length);
  }
  
  void writeFile(String data, String filename) throws IOException {
    File file = new File(filename);
    file.deleteOnExit();
    FileOutputStream os = new FileOutputStream(file);
    os.write(data.getBytes());
    os.close();
  }
  
  void deleteFile(String filename) {
    File file = new File(filename);
    if (file.exists()) {
      file.delete();
    }
  }
}
