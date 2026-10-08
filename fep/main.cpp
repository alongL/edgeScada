// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus - Modern Industrial Edge Data Collector & Gateway Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#include <asio.hpp>
#include <asio/signal_set.hpp>
#include <iostream>
#include <memory>
#include <sstream>

#include "common.hpp"
#include "modbus.hpp"
#include "compact_pack.hpp"
#include "northbound.hpp"
#include "mock_server.hpp"

using namespace edgeplus;

int main(int argc, char* argv[]) {
    std::cout << R"(
  ______     _             _____  _____          _____         ______ ______ _____  
 |  ____|   | |           / ____|/ ____|   /\   |  __ \   /\   |  ____|  ____|  __ \ 
 | |__    __| | __ _  ___| (___ | |       /  \  | |  | | /  \  | |__  | |__  | |__) |
 |  __|  / _` |/ _` |/ _ \\___ \| |      / /\ \ | |  | |/ /\ \ |  __| |  __| |  ___/ 
 | |____| (_| | (_| |  __/____) | |____ / ____ \| |__| / ____ \| |    | |____| |     
 |______|\__,_|\__, |\___|_____/ \_____/_/    \_\_____/_/    \_\_|    |______|_|     
                __/ |                                                                
               |___/    Front-End Processor (FEP) Engine
    )" << std::endl;

    bool enable_mock = true;
    uint16_t web_port = 8080;
    uint16_t modbus_port = 1502;
    std::string central_host = "127.0.0.1";
    uint16_t central_port = 9000;
    std::string collector_id = "collector_01";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-mock") enable_mock = false;
        if (arg == "--web-port" && i + 1 < argc) web_port = std::stoi(argv[++i]);
        if (arg == "--modbus-port" && i + 1 < argc) modbus_port = std::stoi(argv[++i]);
        if (arg == "--central-host" && i + 1 < argc) central_host = argv[++i];
        if (arg == "--central-port" && i + 1 < argc) central_port = std::stoi(argv[++i]);
        if (arg == "--id" && i + 1 < argc) collector_id = argv[++i];
    }

    asio::io_context ctx;

    // 1. 遥测存储转发环形缓冲队列 (容量: 10,000 条)
    RingBuffer<PointValue> telemetry_buffer(10000);

    // 2. 启动内置 Modbus TCP 仿真桩 (模拟逆变器/PLC)
    std::unique_ptr<MockModbusServer> mock_server;
    if (enable_mock) {
        mock_server = std::make_unique<MockModbusServer>(ctx, modbus_port);
        mock_server->Start();
    }

    // 3. 核心采集引擎
    CollectorEngine engine(ctx, telemetry_buffer);

    DeviceConfig dev;
    dev.id = "device_01";
    dev.name = "Solar_Inverter_01";
    dev.ip = "127.0.0.1";
    dev.port = modbus_port;
    dev.slave_id = 1;
    dev.interval_ms = 1000;
    dev.align_second = true;
    dev.max_gap = 20;
    dev.max_regs_per_req = 120;

    // 优先从 CSV 工业点表模板加载 (Excel 导出的 CSV)
    std::string csv_path = "config/templates/inverter_template.csv";
    dev.points = CsvLoader::LoadPointsFromCsv(csv_path);
    if (dev.points.empty()) {
        dev.points = CsvLoader::LoadPointsFromCsv("../config/templates/inverter_template.csv");
    }

    if (!dev.points.empty()) {
        LOG_INFO("[Config] Loaded " << dev.points.size() << " points from CSV template: " << csv_path);
    } else {
        LOG_WARN("[Config] CSV template not found, using fallback points.");
        dev.points = {
            DataPoint{.id = 1,  .name = "active_power",   .function_code = 3, .reg_offset = 100, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = "kW"},
            DataPoint{.id = 2,  .name = "reactive_power", .function_code = 3, .reg_offset = 102, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = "kvar"},
            DataPoint{.id = 3,  .name = "grid_voltage_a", .function_code = 3, .reg_offset = 104, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = "V"},
            DataPoint{.id = 4,  .name = "grid_current_a", .function_code = 3, .reg_offset = 106, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = "A"},
            DataPoint{.id = 5,  .name = "grid_frequency", .function_code = 3, .reg_offset = 108, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = "Hz"},
            DataPoint{.id = 6,  .name = "inverter_temp",  .function_code = 3, .reg_offset = 110, .data_type = DataType::INT16,        .scale = 0.1, .unit = "℃"},
            DataPoint{.id = 7,  .name = "daily_energy",   .function_code = 3, .reg_offset = 112, .data_type = DataType::UINT32_ABCD,  .scale = 0.1, .unit = "kWh"},
            DataPoint{.id = 8,  .name = "running_state",  .function_code = 3, .reg_offset = 120, .bit_offset = 0, .data_type = DataType::BOOLEAN, .unit = ""},
            DataPoint{.id = 9,  .name = "fault_state",    .function_code = 3, .reg_offset = 120, .bit_offset = 1, .data_type = DataType::BOOLEAN, .unit = ""},
            DataPoint{.id = 10, .name = "power_factor",   .function_code = 3, .reg_offset = 122, .data_type = DataType::FLOAT32_CDAB, .scale = 1.0, .unit = ""}
        };
    }

    engine.AddDevice(dev);

    // 4. 北向 CompactPack TCP 客户端 (直连中控服务，纯 KLV 嵌套 + Gzip 压缩流)
    auto forwarder = std::make_shared<TcpForwarder>(ctx, central_host, central_port, collector_id);
    forwarder->Start();

    // 接收中控下发的控制指令 (Pack 格式)
    forwarder->SetCommandCallback([&engine](const CompactPack& cmd_pack) {
        std::string cmd_name = cmd_pack.GetString("cmd");
        if (cmd_name == "CONTROL") {
            ControlCommand cmd;
            cmd.cmd_id = cmd_pack.GetString("cmd_id", "ctrl_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
            cmd.sender = cmd_pack.GetString("sender", "CentralControl");
            cmd.device_id = cmd_pack.GetString("device_id");
            cmd.tag_name = cmd_pack.GetString("tag_name");
            cmd.function_code = static_cast<uint8_t>(cmd_pack.GetInt64("fc", 6));
            cmd.address = static_cast<uint16_t>(cmd_pack.GetInt64("address", 0));
            cmd.numeric_value = cmd_pack.GetDouble("value", 0.0);

            LOG_INFO("[Forwarder] Received Central Control command for " << cmd.device_id << ", tag=" << cmd.tag_name << ", val=" << cmd.numeric_value);
            engine.PushCommand(cmd);
        }
    });

    // 采集完成回调：打包为 CompactPack (外层 Pack 包含测点嵌套 sub-pack)，发送至中控
    engine.SetTelemetryCallback([&ctx, forwarder, collector_id](const std::string& dev_id, int64_t ts_ms, const std::vector<PointValue>& values) {
        CompactPack pack;
        pack.Put("cmd", "TELEMETRY");
        pack.Put("collector_id", collector_id);
        pack.Put("device_id", dev_id);
        pack.PutInt64("ts_ms", ts_ms);
        pack.PutInt64("point_count", static_cast<int64_t>(values.size()));

        // 嵌套测点 Pack: Pack 套 Pack
        CompactPack metrics_pack;
        for (const auto& pv : values) {
            metrics_pack.PutDouble(pv.name, pv.numeric_value);
        }
        pack.PutPack("metrics", metrics_pack);

        asio::co_spawn(ctx, forwarder->AsyncSendPack(pack), asio::detached);
    });

    // 5. 内嵌 Web 监控大屏 (备用或现场直连运维)
    WebServer web_server(ctx, web_port, "./web", engine);
    web_server.Start();

    // 6. 启动采集引擎
    engine.Start();

    // 7. 优雅退出信号监听
    asio::signal_set signals(ctx, SIGINT, SIGTERM);
    signals.async_wait([&](auto, int sig) {
        LOG_INFO("Caught signal " << sig << ", stopping...");
        engine.Stop();
        forwarder->Stop();
        ctx.stop();
    });

    LOG_INFO("EdgePlus is running! Northbound TCP targeting Central Control at " << central_host << ":" << central_port);
    LOG_INFO("Dashboard (local maintenance): http://localhost:" << web_port);
    ctx.run();

    return 0;
}
