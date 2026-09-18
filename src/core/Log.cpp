#include "core/Log.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace bedrock {
namespace logging {

namespace {
std::atomic<int> g_level{static_cast<int>(Level::Info)};
std::mutex g_componentMtx;
std::string g_component = "bedrock";
std::mutex g_writeMtx;
}  // namespace

void setLevel(Level level) { g_level.store(static_cast<int>(level)); }

Level level() { return static_cast<Level>(g_level.load()); }

bool enabled(Level level) { return static_cast<int>(level) <= g_level.load(); }

void setComponent(const std::string& component) {
    std::lock_guard<std::mutex> lk(g_componentMtx);
    g_component = component;
}

std::string timestampTag(const std::string& component) {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const std::time_t seconds = system_clock::to_time_t(now);
    const auto millis = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::tm tm{};
    localtime_r(&seconds, &tm);
    char datePart[32];
    std::strftime(datePart, sizeof(datePart), "%Y-%m-%dT%H:%M:%S", &tm);
    char offsetPart[16];
    const long offsetMinutes = tm.tm_gmtoff / 60;
    if (offsetMinutes == 0) {
        std::snprintf(offsetPart, sizeof(offsetPart), "Z");
    } else {
        const long absMinutes = offsetMinutes < 0 ? -offsetMinutes : offsetMinutes;
        std::snprintf(offsetPart, sizeof(offsetPart), "%c%02ld:%02ld",
                      offsetMinutes < 0 ? '-' : '+', absMinutes / 60, absMinutes % 60);
    }
    char out[96];
    std::snprintf(out, sizeof(out), "[%s %s.%03lld%s]", component.c_str(), datePart,
                  static_cast<long long>(millis), offsetPart);
    return out;
}

std::string timestampTag() {
    std::lock_guard<std::mutex> lk(g_componentMtx);
    return timestampTag(g_component);
}

void write(Level level, const std::string& message) {
    const char* prefix = "";
    switch (level) {
        case Level::Error: prefix = "ERROR "; break;
        case Level::Warn: prefix = "WARN "; break;
        case Level::Info: prefix = ""; break;
        case Level::Debug: prefix = "DEBUG "; break;
    }
    const std::string line = timestampTag() + " " + prefix + message + "\n";
    std::lock_guard<std::mutex> lk(g_writeMtx);
    std::fwrite(line.data(), 1, line.size(), stdout);
    std::fflush(stdout);
}

}  // namespace logging
}  // namespace bedrock
