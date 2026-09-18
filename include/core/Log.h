#pragma once

// Process-wide logging for CppBedrock replicas and the benchmark client.
//
// Every line is prefixed with a timestamped component tag in the same format
// the SmartBFT and Rust harness components use, e.g.
//   [server_2 2026-09-13T20:02:03.123-04:00] Stats view=0 ...
// so the CloudLab plotting scripts can parse replica and client logs
// uniformly. Debug-level lines are compiled in but only emitted when
// bedrock::logging::setLevel(Level::Debug) was called (the --verbose flag).

#include <sstream>
#include <string>

namespace bedrock {
namespace logging {

enum class Level { Error = 0, Warn = 1, Info = 2, Debug = 3 };

void setLevel(Level level);
Level level();
bool enabled(Level level);

// Component name used in the line prefix, e.g. "server_2" or "client".
void setComponent(const std::string& component);

// "[component 2026-09-13T20:02:03.123-04:00]" for the current local time.
std::string timestampTag();
std::string timestampTag(const std::string& component);

// Writes one already-formatted line (without the prefix) to stdout.
void write(Level level, const std::string& message);

}  // namespace logging
}  // namespace bedrock

#define BEDROCK_LOG_AT(lvl, expr)                                              \
    do {                                                                       \
        if (::bedrock::logging::enabled(lvl)) {                                \
            std::ostringstream bedrock_log_os_;                                \
            bedrock_log_os_ << expr;                                           \
            ::bedrock::logging::write(lvl, bedrock_log_os_.str());             \
        }                                                                      \
    } while (0)

#define LOG_ERROR(expr) BEDROCK_LOG_AT(::bedrock::logging::Level::Error, expr)
#define LOG_WARN(expr) BEDROCK_LOG_AT(::bedrock::logging::Level::Warn, expr)
#define LOG_INFO(expr) BEDROCK_LOG_AT(::bedrock::logging::Level::Info, expr)
#define LOG_DEBUG(expr) BEDROCK_LOG_AT(::bedrock::logging::Level::Debug, expr)
