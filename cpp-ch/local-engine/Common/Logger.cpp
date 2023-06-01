#include "Logger.h"

#include <Loggers/Loggers.h>
#include <Poco/AsyncChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/FormattingChannel.h>
#include <Poco/SimpleFileChannel.h>


using Poco::AsyncChannel;
using Poco::AutoPtr;
using Poco::ConsoleChannel;
using Poco::PatternFormatter;
using Poco::FormattingChannel;

void local_engine::Logger::initConsoleLogger(const std::string & level)
{
    AutoPtr<ConsoleChannel> chan(new ConsoleChannel);

    AutoPtr<PatternFormatter> formatter(new PatternFormatter);
    formatter->setProperty("pattern", "%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    formatter->setProperty("times", "local");

    AutoPtr<FormattingChannel> format_channel(new FormattingChannel(formatter, chan));
    AutoPtr<AsyncChannel> async_chann(new AsyncChannel(format_channel));

    Poco::Logger::root().setChannel(async_chann);
    Poco::Logger::root().setLevel(level);
}

void local_engine::Logger::initFileLogger(Poco::Util::AbstractConfiguration & config, const std::string & cmd_name)
{
    static Loggers loggers;
    loggers.buildLoggers(config, Poco::Logger::root(), cmd_name);
}
