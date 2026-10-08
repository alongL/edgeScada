// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus - Modern Industrial Edge Data Collector & Gateway Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <iostream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <mutex>
#include <vector>
#include <string>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <fstream>

#include <asio.hpp>
#include <asio/use_awaitable.hpp>

namespace edgeplus {

// ============================================================================
// 1. 基础数据类型与数据质量定义
// ============================================================================

enum class DataType : uint8_t {
    BOOLEAN,
    INT16,
    UINT16,
    INT32_ABCD,
    INT32_CDAB,
    UINT32_ABCD,
    UINT32_CDAB,
    FLOAT32_ABCD,
    FLOAT32_CDAB,
    STRING
};

enum class Quality : uint8_t {
    BAD = 0,
    UNCERTAIN = 64,
    COMM_FAILURE = 24,
    GOOD = 192
};

struct DataPoint {
    uint32_t    id{0};
    std::string name;
    uint8_t     function_code{3}; // 1: Read Coils, 2: Discrete, 3: Holding, 4: Input
    uint16_t    reg_offset{0};
    int16_t     bit_offset{-1};   // -1: 寄存器整体; >=0: 指定位
    DataType    data_type{DataType::UINT16};
    double      scale{1.0};
    double      offset{0.0};
    std::string unit;
    std::string description;

    uint16_t RegisterCount() const {
        switch (data_type) {
            case DataType::BOOLEAN:
            case DataType::INT16:
            case DataType::UINT16:
                return 1;
            case DataType::INT32_ABCD:
            case DataType::INT32_CDAB:
            case DataType::UINT32_ABCD:
            case DataType::UINT32_CDAB:
            case DataType::FLOAT32_ABCD:
            case DataType::FLOAT32_CDAB:
                return 2;
            case DataType::STRING:
                return 8;
            default:
                return 1;
        }
    }
};

struct PointValue {
    uint32_t    point_id{0};
    std::string name;
    int64_t     timestamp_ms{0};
    double      numeric_value{0.0};
    std::string string_value;
    Quality     quality{Quality::BAD};
    std::string unit;
};

struct DeviceConfig {
    std::string id;
    std::string name;
    std::string ip{"127.0.0.1"};
    uint16_t    port{502};
    uint8_t     slave_id{1};
    uint32_t    interval_ms{1000};
    bool        align_second{true};
    uint16_t    max_gap{20};
    uint16_t    max_regs_per_req{120};
    std::vector<DataPoint> points;
};

struct ControlCommand {
    std::string cmd_id;
    std::string device_id;
    std::string tag_name;
    uint8_t     function_code{6}; // 5: 写单个线圈, 6: 写单个寄存器
    uint16_t    address{0};
    double      numeric_value{0.0};
    std::string sender;
    int64_t     create_time_ms{0};
};

struct CommandAck {
    std::string cmd_id;
    bool        success{false};
    std::string message;
    double      elapsed_ms{0.0};
};

// ============================================================================
// 1.1 工业 CSV 点表与设备模板加载器 (Template-Instance)
// ============================================================================

class CsvLoader {
public:
    static DataType ParseDataType(std::string str) {
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        if (str == "boolean" || str == "bool") return DataType::BOOLEAN;
        if (str == "int16") return DataType::INT16;
        if (str == "uint16") return DataType::UINT16;
        if (str == "int32_cdab") return DataType::INT32_CDAB;
        if (str == "int32" || str == "int32_abcd") return DataType::INT32_ABCD;
        if (str == "uint32_cdab") return DataType::UINT32_CDAB;
        if (str == "uint32" || str == "uint32_abcd") return DataType::UINT32_ABCD;
        if (str == "float32_cdab" || str == "float" || str == "float32") return DataType::FLOAT32_CDAB;
        if (str == "float32_abcd") return DataType::FLOAT32_ABCD;
        if (str == "string") return DataType::STRING;
        return DataType::UINT16;
    }

    // 从 CSV 文件批量解析工业测点
    // 格式: name,function_code,reg_offset[:bit],data_type,scale,offset,unit,description
    static std::vector<DataPoint> LoadPointsFromCsv(const std::string& filepath) {
        std::vector<DataPoint> points;
        std::ifstream file(filepath);
        if (!file.is_open()) return points;

        std::string line;
        uint32_t auto_id = 1;
        bool is_first_line = true;
        while (std::getline(file, line)) {
            // 处理 Windows / Excel 专属的 UTF-8 BOM (0xEF, 0xBB, 0xBF)
            if (is_first_line) {
                is_first_line = false;
                if (line.size() >= 3 &&
                    static_cast<uint8_t>(line[0]) == 0xEF &&
                    static_cast<uint8_t>(line[1]) == 0xBB &&
                    static_cast<uint8_t>(line[2]) == 0xBF) {
                    line = line.substr(3);
                }
            }

            size_t start = line.find_first_not_of(" \t\r\n");
            if (start == std::string::npos || line[start] == '#') continue; // 跳过空行和注释

            std::stringstream ss(line.substr(start));
            std::string item;
            std::vector<std::string> cols;
            while (std::getline(ss, item, ',')) {
                size_t s = item.find_first_not_of(" \t\r\n\"");
                size_t e = item.find_last_not_of(" \t\r\n\"");
                if (s != std::string::npos && e != std::string::npos) {
                    cols.push_back(item.substr(s, e - s + 1));
                } else {
                    cols.push_back("");
                }
            }

            if (cols.size() < 4) continue;

            // 智能识别并跳过表头行 (若第2列不是有效功能码数字，则判定为Excel表头)
            int fc_val = 0;
            try { fc_val = std::stoi(cols[1]); } catch (...) { continue; }
            if (fc_val < 1 || fc_val > 16) continue;

            DataPoint pt;
            pt.id = auto_id++;
            pt.name = cols[0];
            pt.function_code = static_cast<uint8_t>(fc_val);

            std::string reg_str = cols[2];
            auto colon = reg_str.find(':');
            if (colon != std::string::npos) {
                try {
                    pt.reg_offset = static_cast<uint16_t>(std::stoi(reg_str.substr(0, colon)));
                    pt.bit_offset = static_cast<int16_t>(std::stoi(reg_str.substr(colon + 1)));
                } catch (...) { pt.reg_offset = 0; pt.bit_offset = -1; }
            } else {
                try { pt.reg_offset = static_cast<uint16_t>(std::stoi(reg_str)); } catch (...) { pt.reg_offset = 0; }
                pt.bit_offset = -1;
            }

            pt.data_type = ParseDataType(cols[3]);
            if (cols.size() > 4 && !cols[4].empty()) {
                try { pt.scale = std::stod(cols[4]); } catch (...) { pt.scale = 1.0; }
            }
            if (cols.size() > 5 && !cols[5].empty()) {
                try { pt.offset = std::stod(cols[5]); } catch (...) { pt.offset = 0.0; }
            }
            if (cols.size() > 6) pt.unit = cols[6];
            if (cols.size() > 7) pt.description = cols[7];

            points.push_back(std::move(pt));
        }
        return points;
    }
};

// ============================================================================
// 2. 线程安全轻量彩色日志器
// ============================================================================

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

class Logger {
public:
    static Logger& Instance() {
        static Logger instance;
        return instance;
    }

    void SetLevel(LogLevel level) { level_ = level; }
    LogLevel GetLevel() const { return level_; }

    void Log(LogLevel level, const std::string& message) {
        if (level < level_) return;

        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count() % 1000;

        std::tm tm_buf;
#if defined(_WIN32)
        localtime_s(&tm_buf, &now_c);
#else
        localtime_r(&now_c, &tm_buf);
#endif

        const char* color = "";
        const char* reset = "\033[0m";
        const char* tag = "INFO ";

        switch (level) {
            case LogLevel::DEBUG: color = "\033[36m"; tag = "DEBUG"; break;
            case LogLevel::INFO:  color = "\033[32m"; tag = "INFO "; break;
            case LogLevel::WARN:  color = "\033[33m"; tag = "WARN "; break;
            case LogLevel::ERROR: color = "\033[31m"; tag = "ERROR"; break;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << color << "["
                  << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S")
                  << "." << std::setfill('0') << std::setw(3) << now_ms
                  << "] [" << tag << "] "
                  << message << reset << std::endl;
    }

private:
    Logger() = default;
    LogLevel level_{LogLevel::INFO};
    std::mutex mutex_;
};

#define LOG_DEBUG(msg) do { \
    if (::edgeplus::Logger::Instance().GetLevel() <= ::edgeplus::LogLevel::DEBUG) { \
        std::ostringstream _oss; _oss << msg; \
        ::edgeplus::Logger::Instance().Log(::edgeplus::LogLevel::DEBUG, _oss.str()); \
    } \
} while(0)

#define LOG_INFO(msg) do { \
    if (::edgeplus::Logger::Instance().GetLevel() <= ::edgeplus::LogLevel::INFO) { \
        std::ostringstream _oss; _oss << msg; \
        ::edgeplus::Logger::Instance().Log(::edgeplus::LogLevel::INFO, _oss.str()); \
    } \
} while(0)

#define LOG_WARN(msg) do { \
    if (::edgeplus::Logger::Instance().GetLevel() <= ::edgeplus::LogLevel::WARN) { \
        std::ostringstream _oss; _oss << msg; \
        ::edgeplus::Logger::Instance().Log(::edgeplus::LogLevel::WARN, _oss.str()); \
    } \
} while(0)

#define LOG_ERROR(msg) do { \
    if (::edgeplus::Logger::Instance().GetLevel() <= ::edgeplus::LogLevel::ERROR) { \
        std::ostringstream _oss; _oss << msg; \
        ::edgeplus::Logger::Instance().Log(::edgeplus::LogLevel::ERROR, _oss.str()); \
    } \
} while(0)

// ============================================================================
// 3. 高精度整秒对齐与抖动补偿调度器 (Precision Pacer)
// ============================================================================

class PrecisionPacer {
public:
    explicit PrecisionPacer(asio::io_context& ctx, std::chrono::milliseconds interval)
        : timer_(ctx), interval_(interval) {}

    // 系统墙上时钟整数秒对齐 (对齐到下一个 :00.000)
    asio::awaitable<void> AlignToNextSecond() {
        auto now_sys = std::chrono::system_clock::now();
        auto next_sec = std::chrono::time_point_cast<std::chrono::seconds>(now_sys) + std::chrono::seconds(1);
        auto wait_dur = std::chrono::duration_cast<std::chrono::milliseconds>(next_sec - now_sys);
        if (wait_dur.count() > 0) {
            timer_.expires_after(wait_dur);
            co_await timer_.async_wait(asio::use_awaitable);
        }
        next_deadline_ = std::chrono::steady_clock::now();
    }

    // 绝对单调时间推进 (自动扣除采集耗时，将周期抖动控制在 <0.1ms)
    asio::awaitable<void> WaitNextTick() {
        next_deadline_ += interval_;
        timer_.expires_at(next_deadline_);
        co_await timer_.async_wait(asio::use_awaitable);

        auto now = std::chrono::steady_clock::now();
        last_jitter_us_ = std::chrono::duration_cast<std::chrono::microseconds>(now - next_deadline_).count();
    }

    void SetInterval(std::chrono::milliseconds interval) { interval_ = interval; }
    std::chrono::milliseconds GetInterval() const { return interval_; }
    int64_t GetLastJitterUs() const { return last_jitter_us_; }

private:
    asio::steady_timer timer_;
    std::chrono::milliseconds interval_;
    std::chrono::steady_clock::time_point next_deadline_{};
    int64_t last_jitter_us_{0};
};

// ============================================================================
// 4. 线程安全内存环形队列 (存储转发离线缓存)
// ============================================================================

template <typename T>
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity)
        : capacity_(capacity > 0 ? capacity : 100), buffer_(capacity_) {}

    void Push(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        buffer_[head_] = std::move(item);
        head_ = (head_ + 1) % capacity_;
        if (size_ < capacity_) {
            ++size_;
        } else {
            tail_ = (tail_ + 1) % capacity_; // 覆盖最老数据，保证防爆盘
        }
    }

    std::optional<T> Pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == 0) return std::nullopt;
        T item = std::move(buffer_[tail_]);
        tail_ = (tail_ + 1) % capacity_;
        --size_;
        return item;
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_;
    }

    bool Empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_ == 0;
    }

private:
    size_t capacity_;
    std::vector<T> buffer_;
    size_t head_{0};
    size_t tail_{0};
    size_t size_{0};
    mutable std::mutex mutex_;
};

} // namespace edgeplus
