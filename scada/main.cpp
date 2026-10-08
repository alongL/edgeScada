// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus Central Control Server ("中控服务") 主入口
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#include <asio.hpp>
#include <asio/signal_set.hpp>
#include <iostream>

#include "central_server.hpp"

using namespace edgeplus::central;

int main(int argc, char* argv[]) {
    std::cout << R"(
  ______     _             _____  _____          _____         
 |  ____|   | |           / ____|/ ____|   /\   |  __ \   /\   
 | |__    __| | __ _  ___| (___ | |       /  \  | |  | | /  \  
 |  __|  / _` |/ _` |/ _ \\___ \| |      / /\ \ | |  | |/ /\ \ 
 | |____| (_| | (_| |  __/____) | |____ / ____ \| |__| / ____ \
 |______|\__,_|\__, |\___|_____/ \_____/_/    \_\_____/_/    \_
                __/ |                                          
               |___/     Central SCADA Server & Dispatcher
    )" << std::endl;

    uint16_t tcp_port = 9000;
    uint16_t web_port = 8888;
    std::string web_dir = "./web";
    std::string db_path = "data/scada.db";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--tcp-port" && i + 1 < argc) tcp_port = std::stoi(argv[++i]);
        if (arg == "--web-port" && i + 1 < argc) web_port = std::stoi(argv[++i]);
        if (arg == "--web-dir" && i + 1 < argc) web_dir = argv[++i];
        if (arg == "--db" && i + 1 < argc) db_path = argv[++i];
    }

    asio::io_context ctx;

    CentralControlServer server(ctx, tcp_port, web_port, web_dir, db_path);
    server.Start();

    asio::signal_set signals(ctx, SIGINT, SIGTERM);
    signals.async_wait([&](auto, int sig) {
        LOG_INFO("Central Control received signal " << sig << ", stopping...");
        ctx.stop();
    });

    ctx.run();
    return 0;
}
