// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus - Modern Industrial Edge Data Collector & Gateway Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/redirect_error.hpp>

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <deque>
#include <mutex>
#include <atomic>
#include <fstream>
#include <sstream>

#include "common.hpp"
#include "modbus.hpp"
#include "compact_pack.hpp"

namespace edgeplus {

// ============================================================================
// 1. 北向 CompactPack TCP 客户端 (面向中控的高性能 KLV 链路，支持断线重连与离线重发)
// ============================================================================

class TcpForwarder {
public:
    using CommandCallback = std::function<void(const CompactPack&)>;

    TcpForwarder(asio::io_context& ctx, std::string host, uint16_t port, std::string client_id = "collector_01")
        : ctx_(ctx), host_(std::move(host)), port_(port), client_id_(std::move(client_id)), socket_(ctx), timer_(ctx) {}

    ~TcpForwarder() { Stop(); }

    void SetCommandCallback(CommandCallback cb) {
        cmd_cb_ = std::move(cb);
    }

    void Start() {
        is_running_ = true;
        asio::co_spawn(ctx_, ConnectLoop(), asio::detached);
    }

    void Stop() {
        is_running_ = false;
        timer_.cancel();
        if (socket_.is_open()) {
            asio::error_code ec;
            socket_.close(ec);
        }
    }

    bool IsConnected() const {
        return is_connected_;
    }

    // 发送 CompactPack (如果断网则压入本地重传队列)
    asio::awaitable<bool> AsyncSendPack(const CompactPack& pack) {
        auto frame = pack.PackFrame(true); // 自动 Gzip 压缩

        if (!is_connected_) {
            std::lock_guard<std::mutex> lock(offline_mutex_);
            if (offline_queue_.size() < 5000) {
                offline_queue_.push_back(std::move(frame));
            }
            co_return false;
        }

        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(frame), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            LOG_WARN("[TcpForwarder] Write failed: " << ec.message() << ", queueing for replay.");
            is_connected_ = false;
            socket_.close(ec);
            std::lock_guard<std::mutex> lock(offline_mutex_);
            if (offline_queue_.size() < 5000) {
                offline_queue_.push_back(std::move(frame));
            }
            co_return false;
        }
        co_return true;
    }

private:
    asio::awaitable<void> ConnectLoop() {
        while (is_running_) {
            asio::ip::tcp::resolver resolver(ctx_);
            asio::error_code ec;
            auto endpoints = co_await resolver.async_resolve(host_, std::to_string(port_), asio::redirect_error(asio::use_awaitable, ec));

            if (!ec) {
                co_await asio::async_connect(socket_, endpoints, asio::redirect_error(asio::use_awaitable, ec));
            }

            if (!ec) {
                is_connected_ = true;
                LOG_INFO("[TcpForwarder] Connected to Central Control at " << host_ << ":" << port_);

                // 发送上线握手包
                CompactPack handshake;
                handshake.Put("cmd", "HANDSHAKE");
                handshake.Put("client_id", client_id_);
                handshake.Put("proto_ver", "1.0");
                auto hframe = handshake.PackFrame(false);
                co_await asio::async_write(socket_, asio::buffer(hframe), asio::redirect_error(asio::use_awaitable, ec));

                // 补发离线队列
                co_await FlushOfflineQueue();

                // 启动读取循环
                co_await ReadLoop();
            } else {
                LOG_WARN("[TcpForwarder] Connect to " << host_ << ":" << port_ << " failed (" << ec.message() << "), retry in 3s...");
            }

            is_connected_ = false;
            if (socket_.is_open()) socket_.close(ec);

            if (!is_running_) break;

            timer_.expires_after(std::chrono::seconds(3));
            co_await timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        }
    }

    asio::awaitable<void> FlushOfflineQueue() {
        std::vector<std::vector<uint8_t>> batch;
        {
            std::lock_guard<std::mutex> lock(offline_mutex_);
            while (!offline_queue_.empty()) {
                batch.push_back(std::move(offline_queue_.front()));
                offline_queue_.pop_front();
            }
        }
        if (!batch.empty()) {
            LOG_INFO("[TcpForwarder] Replaying " << batch.size() << " offline packets to Central Control...");
            for (const auto& frame : batch) {
                asio::error_code ec;
                co_await asio::async_write(socket_, asio::buffer(frame), asio::redirect_error(asio::use_awaitable, ec));
                if (ec) {
                    is_connected_ = false;
                    socket_.close(ec);
                    // 重新推回队列
                    std::lock_guard<std::mutex> lock(offline_mutex_);
                    offline_queue_.push_front(frame);
                    break;
                }
            }
        }
    }

    asio::awaitable<void> ReadLoop() {
        while (is_connected_) {
            asio::error_code ec;
            // 1. 读取 4 字节负载长度
            uint32_t net_len = 0;
            co_await asio::async_read(socket_, asio::buffer(&net_len, 4), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) {
                LOG_INFO("[TcpForwarder] Disconnected from Central Control: " << ec.message());
                break;
            }

            uint32_t payload_len = ntohl(net_len);
            if (payload_len == 0 || payload_len > 10 * 1024 * 1024) { // 安全上限 10MB
                LOG_ERROR("[TcpForwarder] Invalid frame length: " << payload_len);
                break;
            }

            // 2. 读取负载体 (包含 1 字节 flags + body)
            std::vector<uint8_t> payload(payload_len);
            co_await asio::async_read(socket_, asio::buffer(payload.data(), payload_len), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;

            // 3. 解包 CompactPack
            CompactPack cmd_pack;
            if (cmd_pack.UnpackFramePayload(payload.data(), payload_len)) {
                if (cmd_cb_) {
                    cmd_cb_(cmd_pack);
                }
            } else {
                LOG_WARN("[TcpForwarder] Failed to unpack incoming command frame.");
            }
        }
    }

private:
    asio::io_context& ctx_;
    std::string host_;
    uint16_t port_;
    std::string client_id_;
    asio::ip::tcp::socket socket_;
    asio::steady_timer timer_;
    std::atomic<bool> is_connected_{false};
    std::atomic<bool> is_running_{false};
    CommandCallback cmd_cb_;
    std::deque<std::vector<uint8_t>> offline_queue_;
    std::mutex offline_mutex_;
};

// ============================================================================
// 2. 经典 MQTT 3.1.1 客户端 (保留兼容，支持 QoS 1 与断网离线存储转发)
// ============================================================================

struct MqttMessage {
    std::string topic;
    std::string payload;
    uint8_t qos{0};
};

class MqttClient {
public:
    using MessageCallback = std::function<void(const std::string&, const std::string&)>;

    explicit MqttClient(asio::io_context& ctx) : ctx_(ctx), socket_(ctx) {}
    ~MqttClient() { Disconnect(); }

    void SetMessageCallback(MessageCallback cb) { msg_cb_ = std::move(cb); }

    asio::awaitable<bool> AsyncConnect(
        const std::string& host, uint16_t port = 1883,
        const std::string& client_id = "edgeplus_collector", uint16_t keep_alive_sec = 60)
    {
        host_ = host; port_ = port; client_id_ = client_id; keep_alive_sec_ = keep_alive_sec;
        if (socket_.is_open()) { asio::error_code ec; socket_.close(ec); }

        asio::ip::tcp::resolver resolver(ctx_);
        asio::error_code ec;
        auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) co_return false;

        co_await asio::async_connect(socket_, endpoints, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) co_return false;

        std::vector<uint8_t> var_header = {
            0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02,
            static_cast<uint8_t>(keep_alive_sec >> 8), static_cast<uint8_t>(keep_alive_sec & 0xFF)
        };
        std::vector<uint8_t> payload;
        payload.push_back(static_cast<uint8_t>(client_id_.size() >> 8));
        payload.push_back(static_cast<uint8_t>(client_id_.size() & 0xFF));
        payload.insert(payload.end(), client_id_.begin(), client_id_.end());

        auto connect_pkt = BuildFixedHeader(0x10, var_header.size() + payload.size());
        connect_pkt.insert(connect_pkt.end(), var_header.begin(), var_header.end());
        connect_pkt.insert(connect_pkt.end(), payload.begin(), payload.end());

        co_await asio::async_write(socket_, asio::buffer(connect_pkt), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { socket_.close(ec); co_return false; }

        uint8_t connack[4];
        co_await asio::async_read(socket_, asio::buffer(connack, 4), asio::redirect_error(asio::use_awaitable, ec));
        if (ec || connack[0] != 0x20 || connack[3] != 0x00) { socket_.close(ec); co_return false; }

        is_connected_ = true;
        LOG_INFO("[MQTT] Connected successfully to " << host << ":" << port);
        asio::co_spawn(ctx_, ReadLoop(), asio::detached);
        co_await FlushOfflineQueue();
        co_return true;
    }

    void Disconnect() {
        is_connected_ = false;
        if (socket_.is_open()) {
            asio::error_code ec;
            std::vector<uint8_t> disc = {0xE0, 0x00};
            asio::write(socket_, asio::buffer(disc), ec);
            socket_.close(ec);
        }
    }

    bool IsConnected() const { return is_connected_; }

    asio::awaitable<bool> AsyncPublish(const std::string& topic, const std::string& payload, uint8_t qos = 1) {
        if (!is_connected_) {
            std::lock_guard<std::mutex> lock(offline_mutex_);
            if (offline_queue_.size() < 5000) offline_queue_.push_back({topic, payload, qos});
            co_return false;
        }

        uint8_t hdr = 0x30 | (qos << 1);
        std::vector<uint8_t> var_pld;
        var_pld.push_back(static_cast<uint8_t>(topic.size() >> 8));
        var_pld.push_back(static_cast<uint8_t>(topic.size() & 0xFF));
        var_pld.insert(var_pld.end(), topic.begin(), topic.end());

        if (qos > 0) {
            uint16_t pid = ++packet_id_counter_;
            var_pld.push_back(static_cast<uint8_t>(pid >> 8));
            var_pld.push_back(static_cast<uint8_t>(pid & 0xFF));
        }
        var_pld.insert(var_pld.end(), payload.begin(), payload.end());

        auto pkt = BuildFixedHeader(hdr, var_pld.size());
        pkt.insert(pkt.end(), var_pld.begin(), var_pld.end());

        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(pkt), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) { is_connected_ = false; socket_.close(ec); co_return false; }
        co_return true;
    }

    asio::awaitable<bool> AsyncSubscribe(const std::string& topic, uint8_t qos = 1) {
        if (!is_connected_) co_return false;
        uint16_t pid = ++packet_id_counter_;
        std::vector<uint8_t> var_pld;
        var_pld.push_back(static_cast<uint8_t>(pid >> 8));
        var_pld.push_back(static_cast<uint8_t>(pid & 0xFF));
        var_pld.push_back(static_cast<uint8_t>(topic.size() >> 8));
        var_pld.push_back(static_cast<uint8_t>(topic.size() & 0xFF));
        var_pld.insert(var_pld.end(), topic.begin(), topic.end());
        var_pld.push_back(qos);

        auto pkt = BuildFixedHeader(0x82, var_pld.size());
        pkt.insert(pkt.end(), var_pld.begin(), var_pld.end());

        asio::error_code ec;
        co_await asio::async_write(socket_, asio::buffer(pkt), asio::redirect_error(asio::use_awaitable, ec));
        co_return !ec;
    }

private:
    static std::vector<uint8_t> BuildFixedHeader(uint8_t type, size_t len) {
        std::vector<uint8_t> h = {type};
        do {
            uint8_t b = len % 128; len /= 128;
            if (len > 0) b |= 128;
            h.push_back(b);
        } while (len > 0);
        return h;
    }

    asio::awaitable<void> FlushOfflineQueue() {
        std::vector<MqttMessage> batch;
        {
            std::lock_guard<std::mutex> lock(offline_mutex_);
            while (!offline_queue_.empty()) {
                batch.push_back(std::move(offline_queue_.front()));
                offline_queue_.pop_front();
            }
        }
        if (!batch.empty()) {
            LOG_INFO("[MQTT] Replay " << batch.size() << " offline records...");
            for (const auto& m : batch) co_await AsyncPublish(m.topic, m.payload, m.qos);
        }
    }

    asio::awaitable<void> ReadLoop() {
        while (is_connected_) {
            asio::error_code ec;
            uint8_t fixed_b;
            co_await asio::async_read(socket_, asio::buffer(&fixed_b, 1), asio::redirect_error(asio::use_awaitable, ec));
            if (ec) { is_connected_ = false; break; }

            size_t rem_len = 0, mul = 1;
            uint8_t lb = 0;
            do {
                co_await asio::async_read(socket_, asio::buffer(&lb, 1), asio::redirect_error(asio::use_awaitable, ec));
                if (ec) { is_connected_ = false; break; }
                rem_len += (lb & 127) * mul; mul *= 128;
            } while ((lb & 128) != 0);
            if (!is_connected_) break;

            std::vector<uint8_t> body(rem_len);
            if (rem_len > 0) {
                co_await asio::async_read(socket_, asio::buffer(body.data(), rem_len), asio::redirect_error(asio::use_awaitable, ec));
                if (ec) { is_connected_ = false; break; }
            }

            if ((fixed_b & 0xF0) == 0x30 && body.size() >= 2) {
                uint16_t tlen = (static_cast<uint16_t>(body[0]) << 8) | body[1];
                if (body.size() >= static_cast<size_t>(2 + tlen)) {
                    std::string top(reinterpret_cast<char*>(body.data() + 2), tlen);
                    size_t pld_off = 2 + tlen;
                    uint8_t q = (fixed_b >> 1) & 0x03;
                    if (q > 0 && body.size() >= pld_off + 2) pld_off += 2;
                    std::string pld(reinterpret_cast<char*>(body.data() + pld_off), body.size() - pld_off);
                    if (msg_cb_) msg_cb_(top, pld);
                }
            }
        }
        co_return;
    }

private:
    asio::io_context& ctx_;
    asio::ip::tcp::socket socket_;
    std::string host_; uint16_t port_{1883};
    std::string client_id_; uint16_t keep_alive_sec_{60};
    uint16_t packet_id_counter_{0};
    std::atomic<bool> is_connected_{false};
    MessageCallback msg_cb_;
    std::deque<MqttMessage> offline_queue_;
    mutable std::mutex offline_mutex_;
};

// ============================================================================
// 3. 内嵌轻量 HTTP Web 服务 (大屏看板与 REST 控制 API)
// ============================================================================

class WebServer {
public:
    WebServer(asio::io_context& ctx, uint16_t port, std::string static_root, CollectorEngine& engine)
        : ctx_(ctx), port_(port), static_root_(std::move(static_root)), engine_(engine) {}

    void Start() {
        asio::co_spawn(ctx_, AcceptLoop(), asio::detached);
        LOG_INFO("[Web] Dashboard Server started on http://0.0.0.0:" << port_);
    }

private:
    asio::awaitable<void> AcceptLoop() {
        auto executor = co_await asio::this_coro::executor;
        asio::ip::tcp::acceptor acceptor(executor, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_));
        while (true) {
            asio::error_code ec;
            auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, ec));
            if (!ec) asio::co_spawn(executor, HandleConn(std::move(socket)), asio::detached);
        }
    }

    asio::awaitable<void> HandleConn(asio::ip::tcp::socket socket) {
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
        std::string resp;

        if (ppath == "/" || ppath == "/index.html") {
            std::string fep_file = static_root_ + "/fep_index.html";
            std::ifstream check(fep_file);
            if (check.good()) {
                resp = ServeStatic(fep_file);
            } else {
                resp = ServeStatic(static_root_ + "/index.html");
            }
        } else if (ppath == "/api/status") {
            resp = JsonResp(BuildStatus());
        } else if (ppath == "/api/points") {
            resp = JsonResp(BuildPoints());
        } else if (ppath == "/api/control" && method == "POST") {
            auto bpos = req.find("\r\n\r\n");
            std::string body = (bpos != std::string::npos) ? req.substr(bpos + 4) : "";
            resp = JsonResp(HandleControl(body));
        } else {
            resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found";
        }

        co_await asio::async_write(socket, asio::buffer(resp), asio::redirect_error(asio::use_awaitable, ec));
        socket.close(ec);
        co_return;
    }

    std::string ServeStatic(const std::string& path) {
        std::ifstream f(path, std::ios::binary);
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

    std::string BuildStatus() {
        auto m = engine_.GetDeviceMetrics();
        std::ostringstream ss;
        ss << "{\"service\":\"EdgePlus\",\"devices\":[";
        for (size_t i = 0; i < m.size(); ++i) {
            ss << "{\"id\":\"" << m[i].device_id << "\",\"connected\":" << (m[i].is_connected ? "true" : "false")
               << ",\"success_cycles\":" << m[i].success_cycles << ",\"failed_cycles\":" << m[i].failed_cycles
               << ",\"last_duration_ms\":" << m[i].last_cycle_duration_ms
               << ",\"last_jitter_us\":" << m[i].last_jitter_us
               << ",\"blocks\":" << m[i].coalesced_block_count << ",\"points\":" << m[i].point_count << "}"
               << (i + 1 < m.size() ? "," : "");
        }
        ss << "]}";
        return ss.str();
    }

    std::string BuildPoints() {
        auto pts = engine_.GetLatestPoints();
        std::ostringstream ss;
        ss << "[";
        for (size_t i = 0; i < pts.size(); ++i) {
            ss << "{\"id\":" << pts[i].point_id << ",\"name\":\"" << pts[i].name
               << "\",\"value\":" << pts[i].numeric_value << ",\"quality\":" << (int)pts[i].quality
               << ",\"unit\":\"" << pts[i].unit << "\",\"timestamp_ms\":" << pts[i].timestamp_ms << "}"
               << (i + 1 < pts.size() ? "," : "");
        }
        ss << "]";
        return ss.str();
    }

    std::string HandleControl(const std::string& body) {
        auto get_val = [&](const std::string& k) -> std::string {
            auto p = body.find("\"" + k + "\"");
            if (p == std::string::npos) return "";
            auto col = body.find(':', p);
            if (col == std::string::npos) return "";
            size_t s = body.find_first_not_of(" \t\r\n\"", col + 1);
            size_t e = body.find_first_of(",}\" \t\r\n", s);
            return (s != std::string::npos && e != std::string::npos) ? body.substr(s, e - s) : "";
        };

        ControlCommand cmd;
        cmd.cmd_id = "web_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        cmd.sender = "WebUI";
        cmd.device_id = get_val("device_id");
        cmd.tag_name = get_val("tag_name");
        std::string as = get_val("address"), fs = get_val("fc"), vs = get_val("value");
        cmd.address = as.empty() ? 0 : static_cast<uint16_t>(std::stoi(as));
        cmd.function_code = fs.empty() ? 6 : static_cast<uint8_t>(std::stoi(fs));
        cmd.numeric_value = vs.empty() ? 0.0 : std::stod(vs);

        bool q = engine_.PushCommand(cmd);
        return "{\"status\":\"" + std::string(q ? "QUEUED" : "FAILED") + "\",\"cmd_id\":\"" + cmd.cmd_id + "\"}";
    }

private:
    asio::io_context& ctx_;
    uint16_t port_;
    std::string static_root_;
    CollectorEngine& engine_;
};

} // namespace edgeplus
