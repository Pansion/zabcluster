/*
 * test_log4cplus.cxx
 *
 *  Created on: Jun 14, 2012
 *      Author: pchen
 */

#include "base/logging.h"

int main(int argc, char **argv) {
  DEBUG("debug message");
  INFO("Info message");
  WARN("WARN messgae");
  ERROR("ERROR message");
  FATAL("FATAL message");
  return 0;
}

