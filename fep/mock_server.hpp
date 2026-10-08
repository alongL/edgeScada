#pragma once

#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/redirect_error.hpp>
#include <vector>
#include <cmath>
#include <cstring>
#include <atomic>
#include "common.hpp"

namespace edgeplus {

class MockModbusServer {
public:
    MockModbusServer(asio::io_context& ctx, uint16_t port = 1502)
        : ctx_(ctx), port_(port), registers_(1000, 0)
    {
        // Initialize simulated registers
        UpdateSimulatedValues(0);
    }

    void Start() {
        asio::co_spawn(ctx_, AcceptLoop(), asio::detached);
        asio::co_spawn(ctx_, SimulationTickLoop(), asio::detached);
        LOG_INFO("[Mock] Simulated Modbus TCP Server running on port " << port_);
    }

private:
    void UpdateSimulatedValues(double t) {
        // Reg 100-101: active_power (显著波动: 300kW ~ 1600kW 周期性爬升回落，让动画转速肉眼可见大幅变速)
        // 叠加主周期 + 随机扰动
        float base_wave = 0.5f + 0.5f * std::sin(t * 0.15); // 0.0 ~ 1.0 周期约 40 秒
        float power = 350.0f + 1300.0f * base_wave + 25.0f * std::sin(t * 0.8);
        WriteFloatCDAB(100, power);

        // Reg 102-103: reactive_power (随功率联动波动: 40 ~ 180 kvar)
        float reactive = 50.0f + 120.0f * base_wave + 10.0f * std::cos(t * 0.5);
        WriteFloatCDAB(102, reactive);

        // Reg 104-105: grid_voltage_a (215.0V ~ 228.0V 显著起伏)
        float volt = 220.0f + 6.0f * std::sin(t * 0.25) + 1.2f * std::cos(t * 1.1);
        WriteFloatCDAB(104, volt);

        // Reg 106-107: grid_current_a (随功率与电压联动)
        float curr = power / (volt * 1.732f);
        WriteFloatCDAB(106, curr);

        // Reg 108-109: frequency (49.85Hz ~ 50.18Hz)
        float freq = 50.0f + 0.15f * std::sin(t * 0.3);
        WriteFloatCDAB(108, freq);

        // Reg 110: temp (38.0℃ ~ 49.5℃ 波动，偶尔越过 45℃ 触发真实告警)
        float temp_c = 41.0f + 7.5f * std::sin(t * 0.12);
        registers_[110] = static_cast<uint16_t>(temp_c * 10.0f);

        // Reg 112-113: daily_energy (发电量随功率持续累加)
        daily_energy_accum_ += static_cast<uint32_t>(power * 0.0003f * 10.0f);
        registers_[112] = static_cast<uint16_t>(daily_energy_accum_ >> 16);
        registers_[113] = static_cast<uint16_t>(daily_energy_accum_ & 0xFFFF);

        // Reg 120: Status word (bit 0 = running 1, bit 1 = fault 0)
        registers_[120] = 0x0001;

        // Reg 122-123: power_factor (0.95 ~ 0.99)
        float pf = 0.965f + 0.025f * std::cos(t * 0.2);
        WriteFloatCDAB(122, pf);
    }

    void WriteFloatCDAB(uint16_t reg, float val) {
        uint32_t raw = 0;
        std::memcpy(&raw, &val, sizeof(val));
        // CDAB word swap: high word is low 16-bits, low word is high 16-bits
        registers_[reg] = static_cast<uint16_t>(raw & 0xFFFF);
        registers_[reg + 1] = static_cast<uint16_t>(raw >> 16);
    }

    asio::awaitable<void> SimulationTickLoop() {
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer timer(executor);
        double t = 0.0;
        while (true) {
            timer.expires_after(std::chrono::milliseconds(500));
            co_await timer.async_wait(asio::use_awaitable);
            t += 0.2;
            UpdateSimulatedValues(t);
        }
    }

    asio::awaitable<void> AcceptLoop() {
        auto executor = co_await asio::this_coro::executor;
        asio::ip::tcp::acceptor acceptor(
            executor, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_));

        while (true) {
            asio::error_code ec;
            auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, ec));
            if (!ec) {
                asio::co_spawn(executor, HandleClient(std::move(socket)), asio::detached);
            }
        }
    }

    asio::awaitable<void> HandleClient(asio::ip::tcp::socket socket) {
        while (true) {
            uint8_t mbap[7];
            asio::error_code ec;
            co_await asio::async_read(socket, asio::buffer(mbap, 7),
                                      asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;

            uint16_t trans_id = (static_cast<uint16_t>(mbap[0]) << 8) | mbap[1];
            uint16_t len = (static_cast<uint16_t>(mbap[4]) << 8) | mbap[5];
            uint8_t unit_id = mbap[6];

            if (len < 2) break;
            std::vector<uint8_t> pdu(len - 1);
            co_await asio::async_read(socket, asio::buffer(pdu.data(), pdu.size()),
                                      asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;

            uint8_t fc = pdu[0];
            std::vector<uint8_t> resp;

            if (fc == 3 || fc == 4) { // Read Holding / Input Registers
                uint16_t start_reg = (static_cast<uint16_t>(pdu[1]) << 8) | pdu[2];
                uint16_t count = (static_cast<uint16_t>(pdu[3]) << 8) | pdu[4];
                uint8_t byte_count = static_cast<uint8_t>(count * 2);

                resp.resize(7 + 2 + byte_count);
                // MBAP
                resp[0] = mbap[0]; resp[1] = mbap[1];
                resp[2] = 0; resp[3] = 0;
                uint16_t rlen = 3 + byte_count;
                resp[4] = static_cast<uint8_t>(rlen >> 8);
                resp[5] = static_cast<uint8_t>(rlen & 0xFF);
                resp[6] = unit_id;
                // PDU
                resp[7] = fc;
                resp[8] = byte_count;

                for (size_t i = 0; i < count; ++i) {
                    uint16_t addr = start_reg + i;
                    uint16_t val = (addr < registers_.size()) ? registers_[addr] : 0;
                    resp[9 + i * 2] = static_cast<uint8_t>(val >> 8);
                    resp[9 + i * 2 + 1] = static_cast<uint8_t>(val & 0xFF);
                }
            } else if (fc == 6) { // Write Single Register
                uint16_t addr = (static_cast<uint16_t>(pdu[1]) << 8) | pdu[2];
                uint16_t val = (static_cast<uint16_t>(pdu[3]) << 8) | pdu[4];
                if (addr < registers_.size()) {
                    registers_[addr] = val;
                }
                // Echo request as response
                resp.resize(12);
                std::memcpy(resp.data(), mbap, 7);
                resp[4] = 0; resp[5] = 6;
                resp[7] = fc;
                resp[8] = pdu[1]; resp[9] = pdu[2];
                resp[10] = pdu[3]; resp[11] = pdu[4];
            } else {
                // Exception unsupported
                resp = {mbap[0], mbap[1], 0, 0, 0, 3, unit_id, static_cast<uint8_t>(fc | 0x80), 0x01};
            }

            co_await asio::async_write(socket, asio::buffer(resp),
                                       asio::redirect_error(asio::use_awaitable, ec));
            if (ec) break;
        }
        socket.close();
        co_return;
    }

private:
    asio::io_context& ctx_;
    uint16_t port_;
    std::vector<uint16_t> registers_;
    uint32_t daily_energy_accum_{125000};
};

} // namespace edgeplus
