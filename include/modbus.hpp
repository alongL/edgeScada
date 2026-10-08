// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus - Modern Industrial Edge Data Collector & Gateway Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <vector>
#include <map>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <deque>
#include <functional>
#include <unordered_map>
#include <atomic>

#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/redirect_error.hpp>

#include "common.hpp"

namespace edgeplus {

// ============================================================================
// 1. Modbus 报文组装、解析与工程值解码
// ============================================================================

class ModbusFrame {
public:
    static std::vector<uint8_t> BuildReadRequest(
        uint16_t trans_id, uint8_t slave_id, uint8_t fc, uint16_t start_reg, uint16_t count)
    {
        std::vector<uint8_t> req(12);
        req[0] = static_cast<uint8_t>(trans_id >> 8);
        req[1] = static_cast<uint8_t>(trans_id & 0xFF);
        req[2] = 0x00; req[3] = 0x00;
        req[4] = 0x00; req[5] = 0x06;
        req[6] = slave_id;
        req[7] = fc;
        req[8] = static_cast<uint8_t>(start_reg >> 8);
        req[9] = static_cast<uint8_t>(start_reg & 0xFF);
        req[10] = static_cast<uint8_t>(count >> 8);
        req[11] = static_cast<uint8_t>(count & 0xFF);
        return req;
    }

    static std::vector<uint8_t> BuildWriteCoilRequest(
        uint16_t trans_id, uint8_t slave_id, uint16_t addr, bool value)
    {
        std::vector<uint8_t> req(12);
        req[0] = static_cast<uint8_t>(trans_id >> 8);
        req[1] = static_cast<uint8_t>(trans_id & 0xFF);
        req[2] = 0x00; req[3] = 0x00;
        req[4] = 0x00; req[5] = 0x06;
        req[6] = slave_id;
        req[7] = 0x05;
        req[8] = static_cast<uint8_t>(addr >> 8);
        req[9] = static_cast<uint8_t>(addr & 0xFF);
        req[10] = value ? 0xFF : 0x00;
        req[11] = 0x00;
        return req;
    }

    static std::vector<uint8_t> BuildWriteRegisterRequest(
        uint16_t trans_id, uint8_t slave_id, uint16_t addr, uint16_t value)
    {
        std::vector<uint8_t> req(12);
        req[0] = static_cast<uint8_t>(trans_id >> 8);
        req[1] = static_cast<uint8_t>(trans_id & 0xFF);
        req[2] = 0x00; req[3] = 0x00;
        req[4] = 0x00; req[5] = 0x06;
        req[6] = slave_id;
        req[7] = 0x06;
        req[8] = static_cast<uint8_t>(addr >> 8);
        req[9] = static_cast<uint8_t>(addr & 0xFF);
        req[10] = static_cast<uint8_t>(value >> 8);
        req[11] = static_cast<uint8_t>(value & 0xFF);
        return req;
    }

    static PointValue DecodePoint(
        const DataPoint& pt, uint16_t block_start_reg, const std::vector<uint8_t>& block_data, int64_t ts_ms)
    {
        PointValue pv;
        pv.point_id = pt.id;
        pv.name = pt.name;
        pv.timestamp_ms = ts_ms;
        pv.unit = pt.unit;

        if (pt.reg_offset < block_start_reg) {
            pv.quality = Quality::BAD;
            return pv;
        }

        size_t reg_diff = pt.reg_offset - block_start_reg;

        if (pt.function_code == 1 || pt.function_code == 2) {
            size_t byte_idx = reg_diff / 8;
            uint8_t bit_idx = reg_diff % 8;
            if (byte_idx < block_data.size()) {
                bool bit_val = (block_data[byte_idx] >> bit_idx) & 0x01;
                pv.numeric_value = bit_val ? 1.0 : 0.0;
                pv.quality = Quality::GOOD;
            } else {
                pv.quality = Quality::BAD;
            }
            return pv;
        }

        size_t byte_offset = reg_diff * 2;
        if (byte_offset + pt.RegisterCount() * 2 > block_data.size()) {
            pv.quality = Quality::BAD;
            return pv;
        }

        const uint8_t* p = block_data.data() + byte_offset;

        switch (pt.data_type) {
            case DataType::BOOLEAN: {
                uint16_t reg = (static_cast<uint16_t>(p[0]) << 8) | p[1];
                pv.numeric_value = (pt.bit_offset >= 0 && pt.bit_offset < 16)
                    ? (((reg >> pt.bit_offset) & 1) ? 1.0 : 0.0)
                    : ((reg != 0) ? 1.0 : 0.0);
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::INT16: {
                int16_t raw = static_cast<int16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::UINT16: {
                uint16_t raw = (static_cast<uint16_t>(p[0]) << 8) | p[1];
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::INT32_ABCD: {
                int32_t raw = (static_cast<int32_t>(p[0]) << 24) | (static_cast<int32_t>(p[1]) << 16) |
                              (static_cast<int32_t>(p[2]) << 8)  | static_cast<int32_t>(p[3]);
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::INT32_CDAB: {
                int32_t raw = (static_cast<int32_t>(p[2]) << 24) | (static_cast<int32_t>(p[3]) << 16) |
                              (static_cast<int32_t>(p[0]) << 8)  | static_cast<int32_t>(p[1]);
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::UINT32_ABCD: {
                uint32_t raw = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
                               (static_cast<uint32_t>(p[2]) << 8)  | static_cast<uint32_t>(p[3]);
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::UINT32_CDAB: {
                uint32_t raw = (static_cast<uint32_t>(p[2]) << 24) | (static_cast<uint32_t>(p[3]) << 16) |
                               (static_cast<uint32_t>(p[0]) << 8)  | static_cast<uint32_t>(p[1]);
                pv.numeric_value = raw * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::FLOAT32_ABCD: {
                uint32_t raw = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
                               (static_cast<uint32_t>(p[2]) << 8)  | static_cast<uint32_t>(p[3]);
                float fval = 0.0f;
                std::memcpy(&fval, &raw, sizeof(fval));
                pv.numeric_value = fval * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::FLOAT32_CDAB: {
                uint32_t raw = (static_cast<uint32_t>(p[2]) << 24) | (static_cast<uint32_t>(p[3]) << 16) |
                               (static_cast<uint32_t>(p[0]) << 8)  | static_cast<uint32_t>(p[1]);
                float fval = 0.0f;
                std::memcpy(&fval, &raw, sizeof(fval));
                pv.numeric_value = fval * pt.scale + pt.offset;
                pv.quality = Quality::GOOD;
                break;
            }
            case DataType::STRING: {
                pv.string_value = std::string(reinterpret_cast<const char*>(p), pt.RegisterCount() * 2);
                pv.quality = Quality::GOOD;
                break;
            }
        }
        return pv;
    }
};

// ============================================================================
// 2. Modbus 请求自动合并优化算法 (AutoCoalesce)
// ============================================================================

struct ModbusRequestBlock {
    uint8_t  slave_id{1};
    uint8_t  function_code{3};
    uint16_t start_reg{0};
    uint16_t reg_count{0};
    std::vector<DataPoint> mapped_points;
};

class ModbusCoalescer {
public:
    static std::vector<ModbusRequestBlock> Optimize(
        uint8_t slave_id,
        std::vector<DataPoint> points,
        uint16_t max_gap = 20,
        uint16_t max_regs_per_req = 120)
    {
        std::vector<ModbusRequestBlock> result;
        if (points.empty()) return result;

        std::map<uint8_t, std::vector<DataPoint>> by_fc;
        for (const auto& pt : points) by_fc[pt.function_code].push_back(pt);

        for (auto& [fc, fc_points] : by_fc) {
            std::sort(fc_points.begin(), fc_points.end(), [](const auto& a, const auto& b) {
                if (a.reg_offset != b.reg_offset) return a.reg_offset < b.reg_offset;
                return a.RegisterCount() < b.RegisterCount();
            });

            int cur_start = -1, cur_end = -1;
            std::vector<DataPoint> current_block_points;

            for (const auto& pt : fc_points) {
                int pt_start = pt.reg_offset;
                int pt_end = pt.reg_offset + pt.RegisterCount();

                if (cur_start == -1) {
                    cur_start = pt_start; cur_end = pt_end;
                    current_block_points.push_back(pt);
                } else {
                    int new_end = std::max(cur_end, pt_end);
                    int merged_len = new_end - cur_start;

                    if ((pt_start - cur_end <= max_gap) && (merged_len <= max_regs_per_req)) {
                        cur_end = new_end;
                        current_block_points.push_back(pt);
                    } else {
                        result.push_back({slave_id, fc, static_cast<uint16_t>(cur_start),
                                          static_cast<uint16_t>(cur_end - cur_start),
                                          std::move(current_block_points)});
                        cur_start = pt_start; cur_end = pt_end;
                        current_block_points.clear();
                        current_block_points.push_back(pt);
                    }
                }
            }

            if (cur_start != -1) {
                result.push_back({slave_id, fc, static_cast<uint16_t>(cur_start),
                                  static_cast<uint16_t>(cur_end - cur_start),
                                  std::move(current_block_points)});
            }
        }
        return result;
    }
};

// ============================================================================
// 3. 异步 C++20 协程 Modbus TCP 客户端
// ============================================================================

class ModbusTcpClient {
public:
    explicit ModbusTcpClient(asio::io_context& ctx) : ctx_(ctx), socket_(ctx) {}
    ~ModbusTcpClient() { Close(); }

    asio::awaitable<bool> AsyncConnect(const std::string& host, uint16_t port, uint32_t timeout_ms = 3000) {
        (void)timeout_ms;
        if (socket_.is_open()) { asio::error_code ec; socket_.close(ec); }
        asio::ip::tcp::resolver resolver(ctx_);
        asio::error_code ec;
        auto endpoints = co_await resolver.async_resolve(host, std::to_string(port),
                                                         asio::redirect_error(asio::use_awaitable, ec));
        if (ec) co_return false;

        co_await asio::async_connect(socket_, endpoints, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) co_return false;

        socket_.set_option(asio::ip::tcp::no_delay(true), ec);
        co_return true;
    }

    void Close() {
        if (socket_.is_open()) {
            asio::error_code ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            socket_.close(ec);
        }
    }

    bool IsConnected() const { return socket_.is_open(); }

    asio::awaitable<std::optional<std::vector<uint8_t>>> AsyncReadBlock(
        uint8_t slave_id, uint8_t fc, uint16_t start_reg, uint16_t count)
    {
        if (!IsConnected()) co_return std::nullopt;

        uint16_t trans_id = ++trans_id_counter_;
        auto req = ModbusFrame::BuildReadRequest(trans_id, slave_id, fc, start_reg, count);

        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(req), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { Close(); co_return std::nullopt; }

        uint8_t mbap[7];
        co_await asio::async_read(socket_, asio::buffer(mbap, 7), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { Close(); co_return std::nullopt; }

        uint16_t remaining_len = (static_cast<uint16_t>(mbap[4]) << 8) | mbap[5];
        if (remaining_len < 2 || remaining_len > 260) { Close(); co_return std::nullopt; }

        std::vector<uint8_t> pdu(remaining_len - 1);
        co_await asio::async_read(socket_, asio::buffer(pdu.data(), pdu.size()), asio::redirect_error(asio::use_awaitable, ec));
        if (ec || (pdu[0] & 0x80) || pdu[0] != fc) { Close(); co_return std::nullopt; }

        uint8_t byte_count = pdu[1];
        if (pdu.size() < static_cast<size_t>(2 + byte_count)) { Close(); co_return std::nullopt; }

        co_return std::vector<uint8_t>(pdu.begin() + 2, pdu.begin() + 2 + byte_count);
    }

    asio::awaitable<bool> AsyncWriteCoil(uint8_t slave_id, uint16_t addr, bool value) {
        if (!IsConnected()) co_return false;
        uint16_t trans_id = ++trans_id_counter_;
        auto req = ModbusFrame::BuildWriteCoilRequest(trans_id, slave_id, addr, value);
        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(req), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { Close(); co_return false; }

        uint8_t resp[12];
        co_await asio::async_read(socket_, asio::buffer(resp, 12), asio::redirect_error(asio::use_awaitable, ec));
        co_return (!ec && !(resp[7] & 0x80));
    }

    asio::awaitable<bool> AsyncWriteRegister(uint8_t slave_id, uint16_t addr, uint16_t value) {
        if (!IsConnected()) co_return false;
        uint16_t trans_id = ++trans_id_counter_;
        auto req = ModbusFrame::BuildWriteRegisterRequest(trans_id, slave_id, addr, value);
        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(req), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { Close(); co_return false; }

        uint8_t resp[12];
        co_await asio::async_read(socket_, asio::buffer(resp, 12), asio::redirect_error(asio::use_awaitable, ec));
        co_return (!ec && !(resp[7] & 0x80));
    }

private:
    asio::io_context& ctx_;
    asio::ip::tcp::socket socket_;
    uint16_t trans_id_counter_{0};
};

// ============================================================================
// 4. 多设备调度生命周期与采集引擎 (CollectorEngine)
// ============================================================================

struct DeviceMetrics {
    std::string device_id;
    bool is_connected{false};
    int64_t total_cycles{0};
    int64_t success_cycles{0};
    int64_t failed_cycles{0};
    double last_cycle_duration_ms{0.0};
    int64_t last_jitter_us{0};
    size_t coalesced_block_count{0};
    size_t point_count{0};
};

class CollectorEngine {
public:
    using TelemetryCallback = std::function<void(const std::string&, int64_t, const std::vector<PointValue>&)>;
    using CommandAckCallback = std::function<void(const CommandAck&)>;

    CollectorEngine(asio::io_context& ctx, RingBuffer<PointValue>& buffer)
        : ctx_(ctx), telemetry_buffer_(buffer) {}

    ~CollectorEngine() { Stop(); }

    void AddDevice(DeviceConfig config) { devices_.push_back(std::move(config)); }
    void SetTelemetryCallback(TelemetryCallback cb) { telemetry_cb_ = std::move(cb); }
    void SetCommandAckCallback(CommandAckCallback cb) { cmd_ack_cb_ = std::move(cb); }

    void Start() {
        if (running_) return;
        running_ = true;
        for (auto& dev : devices_) asio::co_spawn(ctx_, DeviceLoop(dev), asio::detached);
        LOG_INFO("CollectorEngine started with " << devices_.size() << " device(s).");
    }

    void Stop() { running_ = false; }

    bool PushCommand(ControlCommand cmd) {
        std::lock_guard<std::mutex> lock(cmd_mutex_);
        cmd_queue_.push_back(std::move(cmd));
        return true;
    }

    std::vector<PointValue> GetLatestPoints() const {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        std::vector<PointValue> res;
        for (const auto& [_, val] : latest_points_) res.push_back(val);
        return res;
    }

    std::vector<DeviceMetrics> GetDeviceMetrics() const {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        std::vector<DeviceMetrics> res;
        for (const auto& [_, m] : device_metrics_) res.push_back(m);
        return res;
    }

private:
    asio::awaitable<void> DeviceLoop(DeviceConfig dev) {
        auto client = std::make_unique<ModbusTcpClient>(ctx_);
        PrecisionPacer pacer(ctx_, std::chrono::milliseconds(dev.interval_ms));
        auto blocks = ModbusCoalescer::Optimize(dev.slave_id, dev.points, dev.max_gap, dev.max_regs_per_req);

        LOG_INFO("[" << dev.id << "] AutoCoalesce generated " << blocks.size()
                 << " request blocks from " << dev.points.size() << " points.");

        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            device_metrics_[dev.id] = {dev.id, false, 0, 0, 0, 0.0, 0, blocks.size(), dev.points.size()};
        }

        if (dev.align_second) {
            co_await pacer.AlignToNextSecond();
            LOG_INFO("[" << dev.id << "] Wall-clock alignment completed. Starting periodic cycles.");
        }

        while (running_) {
            auto cycle_start = std::chrono::steady_clock::now();
            int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            if (!client->IsConnected()) {
                bool ok = co_await client->AsyncConnect(dev.ip, dev.port, 2000);
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                device_metrics_[dev.id].is_connected = ok;
                if (!ok) { co_await pacer.WaitNextTick(); continue; }
            }

            // 优先插队执行外部控制命令
            std::vector<ControlCommand> pending;
            {
                std::lock_guard<std::mutex> lock(cmd_mutex_);
                while (!cmd_queue_.empty()) {
                    if (cmd_queue_.front().device_id == dev.id || cmd_queue_.front().device_id.empty()) {
                        pending.push_back(std::move(cmd_queue_.front()));
                        cmd_queue_.pop_front();
                    } else break;
                }
            }

            for (const auto& cmd : pending) {
                auto cmd_start = std::chrono::steady_clock::now();
                bool ok = (cmd.function_code == 5)
                    ? co_await client->AsyncWriteCoil(dev.slave_id, cmd.address, cmd.numeric_value > 0.5)
                    : co_await client->AsyncWriteRegister(dev.slave_id, cmd.address, static_cast<uint16_t>(cmd.numeric_value));
                auto cmd_end = std::chrono::steady_clock::now();
                double elap = std::chrono::duration<double, std::milli>(cmd_end - cmd_start).count();
                if (cmd_ack_cb_) cmd_ack_cb_({cmd.cmd_id, ok, ok ? "OK" : "Failed", elap});
                LOG_INFO("[" << dev.id << "] Executed command " << cmd.cmd_id << " (" << cmd.tag_name
                         << ") status=" << (ok ? "SUCCESS" : "FAIL") << " elapsed=" << elap << "ms");
            }

            // 执行合并后的周期采集
            std::vector<PointValue> values;
            bool any_fail = false;

            for (const auto& b : blocks) {
                auto raw = co_await client->AsyncReadBlock(b.slave_id, b.function_code, b.start_reg, b.reg_count);
                if (raw) {
                    for (const auto& pt : b.mapped_points)
                        values.push_back(ModbusFrame::DecodePoint(pt, b.start_reg, *raw, now_ms));
                } else {
                    any_fail = true;
                    for (const auto& pt : b.mapped_points)
                        values.push_back({pt.id, pt.name, now_ms, 0.0, "", Quality::COMM_FAILURE, pt.unit});
                }
            }

            auto cycle_end = std::chrono::steady_clock::now();
            double dur_ms = std::chrono::duration<double, std::milli>(cycle_end - cycle_start).count();

            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                for (const auto& v : values) latest_points_[v.name] = v;
                auto& m = device_metrics_[dev.id];
                m.total_cycles++;
                if (any_fail) m.failed_cycles++; else m.success_cycles++;
                m.last_cycle_duration_ms = dur_ms;
                m.last_jitter_us = pacer.GetLastJitterUs();
                m.is_connected = client->IsConnected();
            }

            for (const auto& v : values) telemetry_buffer_.Push(v);
            if (telemetry_cb_ && !values.empty()) telemetry_cb_(dev.id, now_ms, values);

            co_await pacer.WaitNextTick();
        }
        client->Close();
        co_return;
    }

private:
    asio::io_context& ctx_;
    RingBuffer<PointValue>& telemetry_buffer_;
    std::vector<DeviceConfig> devices_;
    std::atomic<bool> running_{false};
    TelemetryCallback telemetry_cb_;
    CommandAckCallback cmd_ack_cb_;

    std::deque<ControlCommand> cmd_queue_;
    mutable std::mutex cmd_mutex_;

    mutable std::mutex snapshot_mutex_;
    std::unordered_map<std::string, PointValue> latest_points_;
    std::unordered_map<std::string, DeviceMetrics> device_metrics_;
};

} // namespace edgeplus
