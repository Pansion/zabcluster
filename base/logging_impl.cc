//Copyright (c) 2012, Pansion Chen <pansion dot zabcpp at gmail dot com>
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//    * Neither the name of the zabcpp nor the
//      names of its contributors may be used to endorse or promote products
//      derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY
//DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "logging_impl.h"
#include "log4cplus/logger.h"
#include "log4cplus/layout.h"
#include "log4cplus/consoleappender.h"
#include "log4cplus/fileappender.h"
#include "log4cplus/loglevel.h"
#include "log4cplus/helpers/pointer.h"
#include "log4cplus/helpers/loglog.h"
#include <string.h>
using namespace std;
using namespace log4cplus::helpers;
using namespace log4cplus;

bool LOG_ON_CONSOLE = true;
static Logger* instance = NULL;
static Logger rootLogger = Logger::getRoot();
const char * LOG_FILE_NAME = "zabcluster";
const int MAX_LOG_SIZE = 10 * 1024 * 1024;
const int MAX_LOG_FILE = 5;
Logger* Logger4cplusWrapper::getLogger() {
  if (instance == NULL) {
    char log[512];
    memset(log, 0, 512);
    snprintf(log, 512, "%s_%d.log", LOG_FILE_NAME, getpid());
    SharedObjectPtr<Appender> appender_file(new RollingFileAppender(LOG4CPLUS_TEXT(log), MAX_LOG_SIZE, MAX_LOG_FILE));
    appender_file->setName(LOG4CPLUS_TEXT("File Logger"));
    log4cplus::tstring pattern = LOG4CPLUS_TEXT("%d{%m/%d/%y %H:%M:%S.%Q} %-5p:%m [pid:%i %l]%n");
    appender_file->setLayout(std::auto_ptr<Layout>(new PatternLayout(pattern)));
    Logger::getRoot().addAppender(appender_file);
    if (LOG_ON_CONSOLE) {
      SharedObjectPtr<Appender> appender_console(new ConsoleAppender());
      appender_console->setName(LOG4CPLUS_TEXT("Console Logger"));
      appender_console->setLayout(std::auto_ptr<Layout>(new PatternLayout(pattern)));
      Logger::getRoot().addAppender(appender_console);
    }
    rootLogger.setLogLevel(ALL_LOG_LEVEL);
    instance = &rootLogger;
  }
  return instance;
}

