package com.pivotal.hamster.appmaster.allocator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;

public class AllocationStrategyFactory {
  private static final Log LOG = LogFactory.getLog(AllocationStrategyFactory.class);

  public static AllocationStrategy getAllocationStrategy(ContainerAllocator allocator, Configuration conf) {
    String strategyType = conf.get(HamsterConfig.ALLOCATION_STRATEGY_KEY, HamsterConfig.DEFAULT_HAMSTER_ALLOCATION_STRATEGY);
    AllocationStrategy strategy = null;
    
    if (StringUtils.equalsIgnoreCase(strategyType, HamsterConfig.PROBABILITY_BASED_ALLOCATION_STRATEGY)) {
      strategy = new ProbabilityBasedAllocationStrategy(allocator, true);
    } else if (StringUtils.equalsIgnoreCase(strategyType, HamsterConfig.USER_POLICY_DRIVEN_ALLOCATION_STRATEGY)) {
      strategy = new UserPolicyStrategy(allocator, true);
    } else if (StringUtils.equalsIgnoreCase(strategyType, HamsterConfig.NAIVE_ALLOCATION_STRATEGY)) {
      strategy = new NaiveAllocationStrategy(allocator, true);
    }
    
    if (strategy == null) {
      throw new HamsterException("got unexpected allocation strategy, what happened?");
    }
    
    LOG.info("get allocation strategy class:" + strategy.getClass().getCanonicalName());
    return strategy;
  }
}
