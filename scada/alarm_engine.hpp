// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus Central Control - Alarm Rule & Evaluation Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>
#include "storage.hpp"

namespace edgeplus::central {

enum class AlarmCondition {
    GREATER_THAN,
    LESS_THAN,
    EQUAL_TO
};

struct AlarmRule {
    std::string point_name;
    AlarmCondition condition;
    double threshold;
    std::string level; // "WARN", "CRITICAL"
    std::string desc;
};

class AlarmEngine {
public:
    explicit AlarmEngine(StorageEngine& storage) : storage_(storage) {
        // 默认内置工业规则 (例如：变流器超温告警、故障状态告警)
        rules_.push_back({
            .point_name = "inverter_temp",
            .condition = AlarmCondition::GREATER_THAN,
            .threshold = 45.0,
            .level = "WARN",
            .desc = "变流器温度偏高 (>45℃)"
        });
        rules_.push_back({
            .point_name = "inverter_temp",
            .condition = AlarmCondition::GREATER_THAN,
            .threshold = 55.0,
            .level = "CRITICAL",
            .desc = "变流器过温超限告警 (>55℃)"
        });
        rules_.push_back({
            .point_name = "fault_state",
            .condition = AlarmCondition::GREATER_THAN,
            .threshold = 0.5,
            .level = "CRITICAL",
            .desc = "机组触发综合故障 (Fault Status = 1)"
        });
    }

    void AddRule(AlarmRule rule) {
        std::lock_guard<std::mutex> lock(mutex_);
        rules_.push_back(std::move(rule));
    }

    void Evaluate(const std::string& device_id, int64_t ts_ms, const std::unordered_map<std::string, double>& metrics) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& r : rules_) {
            auto it = metrics.find(r.point_name);
            if (it == metrics.end()) continue;

            double val = it->second;
            bool triggered = false;
            if (r.condition == AlarmCondition::GREATER_THAN && val > r.threshold) triggered = true;
            else if (r.condition == AlarmCondition::LESS_THAN && val < r.threshold) triggered = true;
            else if (r.condition == AlarmCondition::EQUAL_TO && std::abs(val - r.threshold) < 0.001) triggered = true;

            if (triggered) {
                std::string key = device_id + "_" + r.point_name + "_" + r.level;
                // 防抖：同一告警 10 秒内只记录一次，避免刷屏
                auto last_it = last_alarm_time_.find(key);
                if (last_it == last_alarm_time_.end() || (ts_ms - last_it->second) > 10000) {
                    last_alarm_time_[key] = ts_ms;
                    storage_.InsertAlarm(device_id, r.point_name, r.level, r.desc, val, r.threshold, ts_ms);
                }
            }
        }
    }

private:
    StorageEngine& storage_;
    std::vector<AlarmRule> rules_;
    std::unordered_map<std::string, int64_t> last_alarm_time_;
    std::mutex mutex_;
};

} // namespace edgeplus::central
