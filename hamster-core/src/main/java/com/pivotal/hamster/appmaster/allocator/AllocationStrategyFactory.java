package com.pivotal.hamster.appmaster.allocator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;

public class AllocationStrategyFactory {
  public static AllocationStrategy getAllocationStrategy(ContainerAllocator allocator, Configuration conf) {
    String strategyType = conf.get(HamsterConfig.ALLOCATION_STRATEGY_KEY, HamsterConfig.DEFAULT_HAMSTER_ALLOCATION_STRATEGY);
    if (StringUtils.equalsIgnoreCase(strategyType, HamsterConfig.PROBABILITY_BASED_ALLOCATION_STRATEGY)) {
      return new ProbabilityBasedAllocationStrategy(allocator, true);
    } else if (StringUtils.equalsIgnoreCase(strategyType, HamsterConfig.USER_POLICY_DRIVEN_ALLOCATION_STRATEGY)) {
      return new UserPolicyStrategy(allocator, true);
    }
    
    throw new HamsterException("got unexpected allocation strategy, what happened?");
  }
}
