// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus Central Control Server ("中控服务")
// Clean-room C++20 implementation with SQLite3 Storage & Alarm Engine.
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/redirect_error.hpp>

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>
#include <sstream>
#include <fstream>
#include <iostream>

#include "../include/common.hpp"
#include "../include/compact_pack.hpp"
#include "storage.hpp"
#include "alarm_engine.hpp"

namespace edgeplus::central {

// 遥测数据缓存项
struct TelemetryRecord {
    std::string device_id;
    int64_t timestamp_ms{0};
    std::unordered_map<std::string, double> metrics;
};

// 采集器连接会话
class CollectorSession : public std::enable_shared_from_this<CollectorSession> {
public:
    CollectorSession(asio::ip::tcp::socket socket, 
                     std::function<void(const std::string&, std::shared_ptr<CollectorSession>)> on_register,
                     std::function<void(const std::string&, const TelemetryRecord&)> on_telemetry,
                     std::function<void(const std::string&)> on_disconnect)
        : socket_(std::move(socket)), on_register_(std::move(on_register)),
          on_telemetry_(std::move(on_telemetry)), on_disconnect_(std::move(on_disconnect)) {}

    void Start() {
        auto self = shared_from_this();
        asio::co_spawn(socket_.get_executor(), [this, self]() -> asio::awaitable<void> {
            co_await ReadLoop();
        }, asio::detached);
    }

    asio::awaitable<bool> AsyncSendCommand(const CompactPack& cmd_pack) {
        auto frame = cmd_pack.PackFrame(true);
        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(frame), asio::redirect_error(asio::use_awaitable, ec));
        co_return !ec;
    }

    const std::string& GetCollectorId() const { return collector_id_; }

    void Close() {
        asio::error_code ec;
        if (socket_.is_open()) socket_.close(ec);
    }

private:
    asio::awaitable<void> ReadLoop() {
        while (socket_.is_open()) {
            asio::error_code ec;
            uint32_t net_len = 0;
            co_await asio::async_read(socket_, asio::buffer(&net_len, 4), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;

            uint32_t payload_len = ntohl(net_len);
            if (payload_len == 0 || payload_len > 10 * 1024 * 1024) break;

            std::vector<uint8_t> payload(payload_len);
            co_await asio::async_read(socket_, asio::buffer(payload.data(), payload_len), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;

            CompactPack pack;
            if (pack.UnpackFramePayload(payload.data(), payload_len)) {
                HandlePack(pack);
            }
        }

        if (!collector_id_.empty() && on_disconnect_) {
            on_disconnect_(collector_id_);
        }
    }

    void HandlePack(const CompactPack& pack) {
        std::string cmd = pack.GetString("cmd");
        if (cmd == "HANDSHAKE") {
            collector_id_ = pack.GetString("client_id", "unknown_collector");
            LOG_INFO("[Central Control] Collector registered: " << collector_id_);
            if (on_register_) {
                on_register_(collector_id_, shared_from_this());
            }
        } else if (cmd == "TELEMETRY") {
            TelemetryRecord rec;
            rec.device_id = pack.GetString("device_id");
            rec.timestamp_ms = pack.GetInt64("ts_ms");

            CompactPack metrics_pack;
            if (pack.GetPack("metrics", metrics_pack)) {
                for (const auto& [k, v] : metrics_pack.GetAllFields()) {
                    std::string val_str(reinterpret_cast<const char*>(v.data()), v.size());
                    try {
                        rec.metrics[k] = std::stod(val_str);
                    } catch (...) {
                        rec.metrics[k] = 0.0;
                    }
                }
            }

            if (on_telemetry_) {
                on_telemetry_(collector_id_, rec);
            }
        }
    }

private:
    asio::ip::tcp::socket socket_;
    std::string collector_id_;
    std::function<void(const std::string&, std::shared_ptr<CollectorSession>)> on_register_;
    std::function<void(const std::string&, const TelemetryRecord&)> on_telemetry_;
    std::function<void(const std::string&)> on_disconnect_;
};

// ============================================================================
// CentralControlServer: 中控核心管理调度中心
// ============================================================================

class CentralControlServer {
public:
    CentralControlServer(asio::io_context& ctx, uint16_t tcp_port, uint16_t web_port, std::string web_dir, std::string db_path = "data/scada.db")
        : ctx_(ctx), tcp_port_(tcp_port), web_port_(web_port), web_dir_(std::move(web_dir)),
          db_path_(std::move(db_path)), storage_(db_path_), alarm_engine_(storage_) {}

    void Start() {
        storage_.Open();
        StartTcpServer();
        StartWebServer();
        LOG_INFO("=====================================================================");
        LOG_INFO("  EdgePlus Central SCADA Server Started!");
        LOG_INFO("  -> FEP Southbound TCP:  0.0.0.0:" << tcp_port_ << " (CompactPack KLV+Gzip)");
        LOG_INFO("  -> Northbound Web UI:   http://0.0.0.0:" << web_port_);
        LOG_INFO("  -> SQLite Database:     " << db_path_ << " (Historical & Event Logging)");
        LOG_INFO("=====================================================================");
    }

    asio::awaitable<bool> DispatchControlCommand(const std::string& collector_id, const std::string& device_id,
                                                const std::string& tag, uint8_t fc, uint16_t addr, double val) {
        std::shared_ptr<CollectorSession> session;
        {
            std::lock_guard<std::mutex> lock(collectors_mutex_);
            auto it = collectors_.find(collector_id);
            if (it != collectors_.end()) {
                session = it->second;
            } else if (!collectors_.empty()) {
                session = collectors_.begin()->second; // fallback 到首个在线采集器
            }
        }

        if (!session) co_return false;

        std::string cmd_id = "cc_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        CompactPack cmd_pack;
        cmd_pack.Put("cmd", "CONTROL");
        cmd_pack.Put("cmd_id", cmd_id);
        cmd_pack.Put("sender", "CentralControlWeb");
        cmd_pack.Put("device_id", device_id);
        cmd_pack.Put("tag_name", tag);
        cmd_pack.PutInt64("fc", fc);
        cmd_pack.PutInt64("address", addr);
        cmd_pack.PutDouble("value", val);

        bool sent = co_await session->AsyncSendCommand(cmd_pack);

        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        storage_.InsertControlLog(cmd_id, "WebUI", device_id, tag, fc, addr, val, sent ? "SUCCESS" : "FAILED", now_ms);

        co_return sent;
    }

private:
    void StartTcpServer() {
        asio::co_spawn(ctx_, [this]() -> asio::awaitable<void> {
            auto executor = co_await asio::this_coro::executor;
            asio::ip::tcp::acceptor acceptor(executor, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), tcp_port_));

            while (true) {
                asio::error_code ec;
                auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, ec));
                if (!ec) {
                    auto session = std::make_shared<CollectorSession>(
                        std::move(socket),
                        [this](const std::string& cid, std::shared_ptr<CollectorSession> sess) {
                            OnCollectorRegistered(cid, sess);
                        },
                        [this](const std::string& cid, const TelemetryRecord& rec) {
                            OnTelemetryReceived(cid, rec);
                        },
                        [this](const std::string& cid) {
                            OnCollectorDisconnected(cid);
                        }
                    );
                    session->Start();
                }
            }
        }, asio::detached);
    }

    void OnCollectorRegistered(const std::string& collector_id, std::shared_ptr<CollectorSession> session) {
        std::lock_guard<std::mutex> lock(collectors_mutex_);
        collectors_[collector_id] = session;
        LOG_INFO("[Central Control] Bound collector session for: " << collector_id << " (Total active: " << collectors_.size() << ")");
    }

    void OnTelemetryReceived(const std::string& collector_id, const TelemetryRecord& rec) {
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            latest_telemetry_[rec.device_id] = rec;
            telemetry_count_++;
        }

        // 1. 存入 SQLite 时序数据库
        storage_.InsertTelemetryBatch(rec.device_id, rec.timestamp_ms, rec.metrics);

        // 2. 触发越限告警评估
        alarm_engine_.Evaluate(rec.device_id, rec.timestamp_ms, rec.metrics);
    }

    void OnCollectorDisconnected(const std::string& collector_id) {
        std::lock_guard<std::mutex> lock(collectors_mutex_);
        collectors_.erase(collector_id);
        LOG_INFO("[Central Control] Collector disconnected: " << collector_id);
    }

    void StartWebServer() {
        asio::co_spawn(ctx_, [this]() -> asio::awaitable<void> {
            auto executor = co_await asio::this_coro::executor;
            asio::ip::tcp::acceptor acceptor(executor, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), web_port_));

            while (true) {
                asio::error_code ec;
                auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, ec));
                if (!ec) {
                    asio::co_spawn(executor, HandleHttp(std::move(socket)), asio::detached);
                }
            }
        }, asio::detached);
    }

    asio::awaitable<void> HandleHttp(asio::ip::tcp::socket socket) {
        std::vector<char> buf(4096);
        asio::error_code ec;
        size_t len = co_await socket.async_read_some(asio::buffer(buf), asio::redirect_error(asio::use_awaitable, ec));
        if (ec || len == 0) co_return;

        std::string req(buf.data(), len);
        std::istringstream rss(req);
        std::string method, path, proto;
        rss >> method >> path >> proto;

        auto qp = path.find('?');
        std::string ppath = (qp != std::string::npos) ? path.substr(0, qp) : path;
        std::string query = (qp != std::string::npos) ? path.substr(qp + 1) : "";
        std::string resp;

        if (ppath == "/" || ppath == "/index.html") {
            resp = ServeStatic(web_dir_ + "/index.html");
        } else if (ppath == "/api/status") {
            resp = JsonResp(BuildStatusJson());
        } else if (ppath == "/api/points") {
            resp = JsonResp(BuildPointsJson());
        } else if (ppath == "/api/alarms") {
            resp = JsonResp(BuildAlarmsJson());
        } else if (ppath == "/api/history") {
            resp = JsonResp(BuildHistoryJson(query));
        } else if (ppath == "/api/control" && method == "POST") {
            auto bpos = req.find("\r\n\r\n");
            std::string body = (bpos != std::string::npos) ? req.substr(bpos + 4) : "";
            resp = JsonResp(co_await HandleControlPost(body));
        } else {
            resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found";
        }

        co_await asio::async_write(socket, asio::buffer(resp), asio::redirect_error(asio::use_awaitable, ec));
        socket.close(ec);
    }

    std::string ServeStatic(const std::string& file_path) {
        std::ifstream f(file_path, std::ios::binary);
        if (!f) return "HTTP/1.1 404 Not Found\r\nContent-Length: 14\r\n\r\nFile Not Found";
        std::ostringstream ss; ss << f.rdbuf();
        std::string c = ss.str();
        return "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: " +
               std::to_string(c.size()) + "\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" + c;
    }

    std::string JsonResp(const std::string& j) {
        return "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: " +
               std::to_string(j.size()) + "\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n" + j;
    }

    std::string BuildStatusJson() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        std::ostringstream ss;
        ss << "{\"service\":\"EdgePlus Central SCADA\",\"active_devices\":" << latest_telemetry_.size()
           << ",\"total_packets\":" << telemetry_count_
           << ",\"collectors_count\":" << collectors_.size()
           << ",\"devices\":[";
        size_t idx = 0;
        for (const auto& [dev_id, rec] : latest_telemetry_) {
            ss << "{\"id\":\"" << dev_id << "\",\"connected\":true,\"points\":" << rec.metrics.size()
               << ",\"last_ts\":" << rec.timestamp_ms << "}"
               << (++idx < latest_telemetry_.size() ? "," : "");
        }
        ss << "]}";
        return ss.str();
    }

    std::string BuildPointsJson() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        std::ostringstream ss;
        ss << "[";
        size_t count = 0;
        for (const auto& [dev_id, rec] : latest_telemetry_) {
            for (const auto& [name, val] : rec.metrics) {
                if (count > 0) ss << ",";
                ss << "{\"id\":" << ++count << ",\"device\":\"" << dev_id << "\",\"name\":\"" << name
                   << "\",\"value\":" << val << ",\"quality\":192"
                   << ",\"unit\":\"\",\"timestamp_ms\":" << rec.timestamp_ms << "}";
            }
        }
        ss << "]";
        return ss.str();
    }

    std::string BuildAlarmsJson() {
        auto alarms = storage_.GetRecentAlarms(30);
        std::ostringstream ss;
        ss << "[";
        for (size_t i = 0; i < alarms.size(); ++i) {
            ss << "{\"id\":" << alarms[i].id
               << ",\"device\":\"" << alarms[i].device_id << "\""
               << ",\"point\":\"" << alarms[i].point_name << "\""
               << ",\"level\":\"" << alarms[i].level << "\""
               << ",\"message\":\"" << alarms[i].message << "\""
               << ",\"value\":" << alarms[i].current_value
               << ",\"threshold\":" << alarms[i].threshold
               << ",\"ts\":" << alarms[i].timestamp_ms << "}"
               << (i + 1 < alarms.size() ? "," : "");
        }
        ss << "]";
        return ss.str();
    }

    std::string BuildHistoryJson(const std::string& query) {
        auto get_param = [&](const std::string& k) -> std::string {
            auto p = query.find(k + "=");
            if (p == std::string::npos) return "";
            auto e = query.find('&', p);
            return (e == std::string::npos) ? query.substr(p + k.size() + 1) : query.substr(p + k.size() + 1, e - (p + k.size() + 1));
        };
        std::string dev = get_param("device");
        std::string point = get_param("point");
        if (dev.empty()) dev = "device_01";
        if (point.empty()) point = "active_power";

        auto hist = storage_.GetHistory(dev, point, 60);
        std::ostringstream ss;
        ss << "[";
        for (size_t i = 0; i < hist.size(); ++i) {
            ss << "{\"ts\":" << hist[i].timestamp_ms << ",\"value\":" << hist[i].value << "}"
               << (i + 1 < hist.size() ? "," : "");
        }
        ss << "]";
        return ss.str();
    }

    asio::awaitable<std::string> HandleControlPost(const std::string& body) {
        auto get_val = [&](const std::string& k) -> std::string {
            auto p = body.find("\"" + k + "\"");
            if (p == std::string::npos) return "";
            auto col = body.find(':', p);
            if (col == std::string::npos) return "";
            size_t s = body.find_first_not_of(" \t\r\n\"", col + 1);
            size_t e = body.find_first_of(",}\" \t\r\n", s);
            return (s != std::string::npos && e != std::string::npos) ? body.substr(s, e - s) : "";
        };

        std::string collector_id = get_val("collector_id");
        std::string dev_id = get_val("device_id");
        std::string tag = get_val("tag_name");
        std::string as = get_val("address"), fs = get_val("fc"), vs = get_val("value");
        uint16_t addr = as.empty() ? 0 : static_cast<uint16_t>(std::stoi(as));
        uint8_t fc = fs.empty() ? 6 : static_cast<uint8_t>(std::stoi(fs));
        double val = vs.empty() ? 0.0 : std::stod(vs);

        bool sent = co_await DispatchControlCommand(collector_id, dev_id, tag, fc, addr, val);
        co_return "{\"status\":\"" + std::string(sent ? "FORWARDED" : "OFFLINE") + "\"}";
    }

private:
    asio::io_context& ctx_;
    uint16_t tcp_port_{9000};
    uint16_t web_port_{8888};
    std::string web_dir_;
    std::string db_path_{"data/scada.db"};
    StorageEngine storage_;
    AlarmEngine alarm_engine_;
    std::unordered_map<std::string, std::shared_ptr<CollectorSession>> collectors_;
    std::mutex collectors_mutex_;
    std::unordered_map<std::string, TelemetryRecord> latest_telemetry_;
    std::atomic<uint64_t> telemetry_count_{0};
    std::mutex data_mutex_;
};

} // namespace edgeplus::central
