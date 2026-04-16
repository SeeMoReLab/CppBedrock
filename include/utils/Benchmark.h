#pragma once
#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <numeric>
#include <algorithm>
#include <iostream>
#include <fstream>

class Benchmark {
public:
    explicit Benchmark(std::string name = "bench")
        : _name(std::move(name)), _t0(Clock::now()) {}

    void start(const std::string& id) {
        auto now = Clock::now();
        std::lock_guard<std::mutex> lg(_mtx);
        _starts[id] = now;
        ++_issued;
    }
    void end(const std::string& id) {
        auto now = Clock::now();
        std::lock_guard<std::mutex> lg(_mtx);
        auto it = _starts.find(id);
        if (it == _starts.end()) return;
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(now - it->second).count();
        _latencies.push_back(us);
        _starts.erase(it);
        ++_completed;
    }
    void markError(const std::string& id) {
        std::lock_guard<std::mutex> lg(_mtx);
        if (_starts.erase(id)) ++_errors;
    }
    void finishRun() { _t1 = Clock::now(); }

    void reset(const std::string& name) {
        std::lock_guard<std::mutex> lg(_mtx);
        _name = name;
        _t0 = Clock::now();
        _t1 = Clock::time_point{};
        _starts.clear();
        _latencies.clear();
        _issued = _completed = _errors = 0;
    }

    void printSummary(std::ostream& os = std::cout) {
        auto s = stats();
        os << "\n==== Benchmark " << _name << " ====\n";
        os << "Issued=" << s.issued << " Completed=" << s.completed
           << " Errors=" << s.errors << " InFlight=" << s.in_flight << "\n";
        os << "Latency(us): avg=" << s.avg
           << " p50=" << s.p50 << " p90=" << s.p90 << " p99=" << s.p99
           << " min=" << s.min << " max=" << s.max << "\n";
        os << "Throughput=" << s.throughput << " ops/sec\n";
        os << "==============================\n";
    }
    void record(long latency_us) {
        std::lock_guard<std::mutex> lg(_mtx);
        _latencies.push_back(latency_us);
        ++_issued;
        ++_completed;
    }
    void exportCSV(const std::string& path) {
        std::lock_guard<std::mutex> lg(_mtx);
        std::ofstream out(path);
        out << "latency_us\n";
        for (auto v : _latencies) out << v << "\n";
    }

    struct Stats {
        double avg=0; long p50=0,p90=0,p99=0,min=0,max=0;
        double throughput=0;
        size_t issued=0,completed=0,errors=0,in_flight=0;
    };
    Stats stats() {
        std::lock_guard<std::mutex> lg(_mtx);
        Stats s;
        s.issued=_issued; s.completed=_completed; s.errors=_errors; s.in_flight=_starts.size();
        auto end = (_t1.time_since_epoch().count()==0)?Clock::now():_t1;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - _t0).count();
        if (ms>0) s.throughput = (double)_completed * 1000.0 / (double)ms;
        if (_latencies.empty()) return s;
        std::vector<long> v=_latencies;
        std::sort(v.begin(), v.end());
        s.min=v.front(); s.max=v.back();
        s.avg = (double)std::accumulate(v.begin(), v.end(), 0LL)/v.size();
        s.p50=pct(v,0.50); s.p90=pct(v,0.90); s.p99=pct(v,0.99);
        return s;
    }

private:
    using Clock = std::chrono::steady_clock;
    static long pct(const std::vector<long>& v,double p){
        if (v.empty()) return 0;
        double idx = p * (v.size()-1);
        size_t lo = (size_t)idx;
        size_t hi = std::min(v.size()-1, lo+1);
        double f = idx - lo;
        return (long)((1.0-f)*v[lo] + f*v[hi]);
    }
    std::string _name;
    Clock::time_point _t0;
    Clock::time_point _t1{};
    std::unordered_map<std::string, Clock::time_point> _starts;
    std::vector<long> _latencies;
    size_t _issued=0,_completed=0,_errors=0;
    std::mutex _mtx;
}
;