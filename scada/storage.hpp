// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus Central Control - SQLite3 Historical & Event Storage
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <filesystem>
#include <sqlite3.h>
#include <string>
#include <vector>
#include <mutex>
#include <iostream>
#include <sstream>

#include "common.hpp"

namespace edgeplus::central {

struct HistoryRecord {
    std::string device_id;
    std::string point_name;
    double value{0.0};
    int64_t timestamp_ms{0};
};

struct AlarmRecord {
    int64_t id{0};
    std::string device_id;
    std::string point_name;
    std::string level; // "WARN", "CRITICAL"
    std::string message;
    double current_value{0.0};
    double threshold{0.0};
    int64_t timestamp_ms{0};
};

class StorageEngine {
public:
    explicit StorageEngine(const std::string& db_path = "data/scada.db")
        : db_path_(db_path) {}

    ~StorageEngine() {
        Close();
    }

    bool Open() {
        std::lock_guard<std::mutex> lock(mutex_);

        // 自动创建数据库父级目录 (例如 data/)
        try {
            std::filesystem::path p(db_path_);
            if (p.has_parent_path()) {
                std::filesystem::create_directories(p.parent_path());
            }
        } catch (const std::exception& e) {
            LOG_WARN("[Storage] Failed to create directories for " << db_path_ << ": " << e.what());
        }

        if (sqlite3_open(db_path_.c_str(), &db_) != SQLITE_OK) {
            LOG_ERROR("[Storage] Failed to open database: " << sqlite3_errmsg(db_));
            return false;
        }

        // 优化写入性能: WAL 模式 + NORMAL 同步
        ExecuteSqlNoLock("PRAGMA journal_mode = WAL;");
        ExecuteSqlNoLock("PRAGMA synchronous = NORMAL;");

        // 1. 遥测历史序列表
        const char* create_telemetry = R"(
            CREATE TABLE IF NOT EXISTS telemetry_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                point_name TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry_history(device_id, point_name, timestamp_ms);
        )";

        // 2. 告警事件表
        const char* create_alarms = R"(
            CREATE TABLE IF NOT EXISTS alarms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                point_name TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                current_value REAL NOT NULL,
                threshold REAL NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alarms_ts ON alarms(timestamp_ms);
        )";

        // 3. 操作与控制日志表
        const char* create_controls = R"(
            CREATE TABLE IF NOT EXISTS control_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cmd_id TEXT NOT NULL,
                sender TEXT NOT NULL,
                device_id TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                fc INTEGER NOT NULL,
                address INTEGER NOT NULL,
                value REAL NOT NULL,
                status TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
        )";

        ExecuteSqlNoLock(create_telemetry);
        ExecuteSqlNoLock(create_alarms);
        ExecuteSqlNoLock(create_controls);

        LOG_INFO("[Storage] SQLite database opened and initialized at: " << db_path_);
        return true;
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    // 批量写入遥测点位 (在一个事务中完成)
    void InsertTelemetryBatch(const std::string& device_id, int64_t ts_ms, const std::unordered_map<std::string, double>& metrics) {
        if (!db_ || metrics.empty()) return;
        std::lock_guard<std::mutex> lock(mutex_);

        sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

        const char* sql = "INSERT INTO telemetry_history (device_id, point_name, value, timestamp_ms) VALUES (?, ?, ?, ?);";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            for (const auto& [name, val] : metrics) {
                sqlite3_bind_text(stmt, 1, device_id.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, name.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_double(stmt, 3, val);
                sqlite3_bind_int64(stmt, 4, ts_ms);
                sqlite3_step(stmt);
                sqlite3_reset(stmt);
            }
            sqlite3_finalize(stmt);
        }

        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    // 记录告警
    void InsertAlarm(const std::string& device_id, const std::string& point_name,
                     const std::string& level, const std::string& message,
                     double cur_val, double threshold, int64_t ts_ms) {
        if (!db_) return;
        std::lock_guard<std::mutex> lock(mutex_);

        const char* sql = "INSERT INTO alarms (device_id, point_name, level, message, current_value, threshold, timestamp_ms) VALUES (?, ?, ?, ?, ?, ?, ?);";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, device_id.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, point_name.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, level.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, message.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(stmt, 5, cur_val);
            sqlite3_bind_double(stmt, 6, threshold);
            sqlite3_bind_int64(stmt, 7, ts_ms);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    // 记录控制指令
    void InsertControlLog(const std::string& cmd_id, const std::string& sender,
                          const std::string& device_id, const std::string& tag_name,
                          uint8_t fc, uint16_t addr, double val, const std::string& status, int64_t ts_ms) {
        if (!db_) return;
        std::lock_guard<std::mutex> lock(mutex_);

        const char* sql = "INSERT INTO control_logs (cmd_id, sender, device_id, tag_name, fc, address, value, status, timestamp_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, cmd_id.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, sender.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, device_id.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, tag_name.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_int(stmt, 5, fc);
            sqlite3_bind_int(stmt, 6, addr);
            sqlite3_bind_double(stmt, 7, val);
            sqlite3_bind_text(stmt, 8, status.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_int64(stmt, 9, ts_ms);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    // 查询最近告警
    std::vector<AlarmRecord> GetRecentAlarms(size_t limit = 50) {
        std::vector<AlarmRecord> res;
        if (!db_) return res;
        std::lock_guard<std::mutex> lock(mutex_);

        std::string sql = "SELECT id, device_id, point_name, level, message, current_value, threshold, timestamp_ms FROM alarms ORDER BY id DESC LIMIT " + std::to_string(limit) + ";";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                AlarmRecord a;
                a.id = sqlite3_column_int64(stmt, 0);
                a.device_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                a.point_name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                a.level = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
                a.message = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
                a.current_value = sqlite3_column_double(stmt, 5);
                a.threshold = sqlite3_column_double(stmt, 6);
                a.timestamp_ms = sqlite3_column_int64(stmt, 7);
                res.push_back(std::move(a));
            }
            sqlite3_finalize(stmt);
        }
        return res;
    }

    // 查询特定测点的历史时序数据
    std::vector<HistoryRecord> GetHistory(const std::string& device_id, const std::string& point_name, size_t limit = 100) {
        std::vector<HistoryRecord> res;
        if (!db_) return res;
        std::lock_guard<std::mutex> lock(mutex_);

        std::string sql = "SELECT device_id, point_name, value, timestamp_ms FROM telemetry_history WHERE device_id = ? AND point_name = ? ORDER BY id DESC LIMIT " + std::to_string(limit) + ";";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, device_id.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, point_name.c_str(), -1, SQLITE_STATIC);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                HistoryRecord h;
                h.device_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                h.point_name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                h.value = sqlite3_column_double(stmt, 2);
                h.timestamp_ms = sqlite3_column_int64(stmt, 3);
                res.push_back(std::move(h));
            }
            sqlite3_finalize(stmt);
        }
        return res;
    }

private:
    void ExecuteSqlNoLock(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            LOG_WARN("[Storage] SQL exec warning: " << (err ? err : "unknown"));
            sqlite3_free(err);
        }
    }

private:
    std::string db_path_;
    sqlite3* db_{nullptr};
    std::mutex mutex_;
};

} // namespace edgeplus::central
