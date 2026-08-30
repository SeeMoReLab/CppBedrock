#include "../../../include/core/state/DataSet.h"
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>

void DataSet::setRecords(const std::unordered_map<std::string, nlohmann::json>& records) {
    std::lock_guard<std::mutex> lock(_mtx);
    _records = records;
}

std::unordered_map<std::string, nlohmann::json> DataSet::getRecords() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _records;
}

void DataSet::update(const std::string& key, const nlohmann::json& value) {
    std::lock_guard<std::mutex> lock(_mtx);
    constexpr size_t MAX_RECORDS = 100000; // or a value suitable for your workload
    if (_records.size() >= MAX_RECORDS && _records.find(key) == _records.end()) {
        std::cerr << "[DataSet] Max record limit reached, not inserting key: " << key << std::endl;
        return;
    }
    _records[key] = value;
}

nlohmann::json DataSet::get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _records.find(key);
    if (it != _records.end()) return it->second;
    return nullptr;
}

void DataSet::saveToFile(const std::string& filename) const {
    std::lock_guard<std::mutex> lock(_mtx);
    nlohmann::json j(_records);
    std::string tmpFilename = filename + ".tmp";
    {
        std::ofstream out(tmpFilename, std::ios::trunc);
        if (out.is_open()) {
            out << j.dump(4) << std::endl;
        }
    }
    // Atomically replace the old file
    std::rename(tmpFilename.c_str(), filename.c_str());
}

void DataSet::loadFromFile(const std::string& filename) {
    std::lock_guard<std::mutex> lock(_mtx);
    std::ifstream in(filename);
    if (in.is_open()) {
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (!content.empty()) {
            try {
                nlohmann::json j = nlohmann::json::parse(content);
                _records = j.get<std::unordered_map<std::string, nlohmann::json>>();
            } catch (const nlohmann::json::parse_error& e) {
                std::cerr << "[DataSet] Failed to parse JSON from " << filename << ": " << e.what() << std::endl;
                _records.clear(); // Optionally clear or keep old state
            }
        }
    }
}

bool DataSet::exists(const std::string& key) const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _records.find(key) != _records.end();
}