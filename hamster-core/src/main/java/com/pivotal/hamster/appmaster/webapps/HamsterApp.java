package com.pivotal.hamster.appmaster.webapps;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

@RequestScoped
public class HamsterApp {
  @Inject
  HamsterApp() {
  }
}
