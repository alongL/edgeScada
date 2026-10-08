# EdgeSCADA ⚡

> **Modern Industrial SCADA Suite & High-Performance Edge Engine**  
> 基于纯现代 **C++20 Asio 协程** 构建的工业级 SCADA 监控调度套件与高精度边缘采集引擎。包含 SCADA 中控调度服务器 (`edgescada`)、现场通信前置机引擎 (`edgescada-fep`)、CompactPack 二进制紧凑嵌套流、SQLite3 历史时序与审计存储、实时越限告警评估引擎及现代化 Web 监控调度大屏。

[![License](https://img.shields.io/badge/License-GPL_v3-orange.svg)](LICENSE)
[![Standard](https://img.shields.io/badge/C%2B%2B-20-purple.svg)](https://en.wikipedia.org/wiki/C%2B%2B20)
[![Asio](https://img.shields.io/badge/Asio-Standalone-green.svg)](https://think-async.com/Asio/)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows-lightgrey.svg)](README.md)

---

## 📖 目录 (Table of Contents)

- [1. 系统定位与核心架构](#1-系统定位与核心架构)
  - [1.1 整体分层架构](#11-整体分层架构)
  - [1.2 SCADA 中控系统 (edgescada)](#12-scada-中控系统-edgescada)
  - [1.3 前置采集引擎 (edgescada-fep)](#13-前置采集引擎-edgescada-fep)

- [2. 关键核心机制](#2-关键核心机制)
  - [2.1 微秒级整秒时钟律动 (Precision Pacer)](#21-微秒级整秒时钟律动-precision-pacer)
  - [2.2 Modbus 请求自动合并算法 (AutoCoalesce)](#22-modbus-请求自动合并算法-autocoalesce)
  - [2.3 集中控制指令毫秒级优先插队](#23-集中控制指令毫秒级优先插队)
  - [2.4 实时越限告警引擎与 SQLite3 归档](#24-实时越限告警引擎与-sqlite3-归档)
- [3. 编译与安装指南](#3-编译与安装指南)
- [4. 使用指南与命令行参数](#4-使用指南与命令行参数)
- [5. 海量测点与设备配置指南 (CSV 模板化架构)](#5-海量测点与设备配置指南-csv-模板化架构)
- [6. Web 监控大屏与 REST API](#6-web-监控大屏与-rest-api)
- [7. 开源协议与商业授权声明](#7-开源协议与商业授权声明)

---

## 1. 系统定位与核心架构

在传统工业自动化与新能源（风电、光伏、储能、PLC）控制系统中，老旧 SCADA 与上位机系统面临诸多痛点：
1. **系统臃肿庞大**：传统组态软件动辄几十 GB，资源开销极大，无法高效运行于现代工业边缘计算网关或轻量化服务器；
2. **时钟抖动严重**：相对延时调度累积网络开销，导致周期严重漂移（几十毫秒抖动），破坏了多机组多设备在同一秒级的对齐度；
3. **协议繁冗低效**：现场大量使用明文或 JSON，不仅传输带宽开销大，且序列化与反序列化解析消耗大量 CPU。

**EdgeSCADA** 采用现代 C++20 协程纯净重构，将系统精炼为**前置通信采集（FEP）**与**中控监控调度中心（Server）**两层紧密协作体系。

### 1.1 整体分层架构

```
┌────────────────────────────────────────────────────────┐
│                   工业现场设备 (PLC / 逆变器 / 传感器)    │
└───────────────────────────┬────────────────────────────┘
                            │ Modbus TCP (标准 502 或自定义端口)
                            ▼
┌────────────────────────────────────────────────────────┐
│             edgescada-fep 前置机采集引擎 (FEP)           │
│                                                        │
│  ┌───────────────────────┐   ┌───────────────────────┐ │
│  │ PrecisionPacer 调度器 │   │ AutoCoalesce 合并算法 │ │
│  │ (系统整秒对齐+抖动扣除)│   │ (区间聚类, 消除总线冗余)│ │
│  └───────────────────────┘   └───────────────────────┘ │
│  ┌───────────────────────┐   ┌───────────────────────┐ │
│  │ ModbusTcpClient 协程  │   │ 优先级指令插队队列    │ │
│  │ (纯异步非阻塞 IO)     │   │ (下发指令毫秒级优先抢占)│ │
│  └───────────────────────┘   └───────────────────────┘ │
└───────────────────────────┬────────────────────────────┘
                            │ 北向 CompactPack KLV+Gzip 压缩 TCP 流
                            ▼
┌────────────────────────────────────────────────────────┐
│             edgescada 中控调度服务器 (SCADA Server)     │
│                                                        │
│  ┌────────────────────────┐  ┌───────────────────────┐ │
│  │ KLV 汇聚与测点镜像中心 │  │ 实时越限告警引擎      │ │
│  └────────────────────────┘  └───────────────────────┘ │
│  ┌────────────────────────┐  ┌───────────────────────┐ │
│  │ SQLite3 历史时序/事件表 │  │ 集中控制指令下发路由  │ │
│  └────────────────────────┘  └───────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  内嵌高并发 Web 监控调度大屏与 REST API (端口 8888)  │ │
│  └───────────────────────────────────────────────────┘ │
└───────────────────────────┬────────────────────────────┘
                            │ 浏览器 HTTP / REST 访问
                            ▼
                     [ 调度工程师大屏 / Web 客户端 ]
```

### 1.2 SCADA 中控系统 (`edgescada`)
- **连接管理**：维护各 FEP 前置机的上线握手与长连接健康状态；
- **实时测点镜像**：内存级维护整个场站所有机组/设备的最新测点实时库；
- **告警与事件中心**：内置规则引擎，自动检测越限（如变流器超温、综合故障触发），带防抖滤波；
- **数据归档存储**：零运维嵌入式 SQLite3 引擎，开启 WAL 模式高性能持久化时序数据与操作审计日志；
- **Web 调度大屏**：开箱即用响应式前端看板，直接展示实时测点矩阵、实时告警、历史采样曲线及远程下发控制。

### 1.3 前置采集引擎 (`edgescada-fep`)
- 下沉至工业机柜内部，直连 PLC/变流器等物理 Modbus 设备；
- 专注硬核高精度周期采集，绝不对外暴露多余业务端口；
- 具备断网本地排队与网络恢复重传机制，支持在 8080 端口提供现场轻量自检。

### 1.4 CompactPack 紧凑二进制协议 (KLV + Gzip)
抛弃 JSON 冗余的引号、花括号和转义符语法噪音，专为工业高频传输定制：
- 字段紧凑排列：`[uint16 key_len][key][uint32 val_len][val]`；
- **Pack 套 Pack 递归嵌套**：子 Pack（如一组测点矩阵）可直接序列化存入父 Pack 字段；
- **透明 Gzip 压缩网络帧**：超过 64 字节自动启用 zlib 硬件级高效压缩，极大节省工业专线带宽。

---

## 2. 关键核心机制

### 2.1 微秒级整秒时钟律动 (Precision Pacer)
传统轮询采用相对睡眠 `sleep(1000ms)`，每次网络和逻辑耗时 $T_{exec}$ 都会累加，导致时间严重漂移。
EdgeSCADA 采用**双阶段精准对齐算法**：
1. **整秒硬对齐 (Wall-Clock Alignment)**：启动时精确计算距离当前系统墙上时钟下一个整数秒边界（`:00.000`）的纳秒差值，等待至绝对整秒瞬间触发；
2. **绝对单调时间推进 (Monotonic Absolute Pacing)**：锚定 `std::chrono::steady_clock`，每次推进固定周期：
   $$\text{Next Deadline} = \text{Next Deadline} + \Delta t$$
   即使某次采集因为网络耗时 $15\text{ms}$，调度器依然以预定时间点准时触发下一次，**自动扣除执行耗时，消除累积误差，周期抖动控制在 $<0.1\text{ms}$（实测约 $0.05\sim 0.15\text{ms}$）**。

### 2.2 Modbus 请求自动合并算法 (AutoCoalesce)
对于现场成百上千的离散测点，手动分包繁琐易错。`AutoCoalesce` 算法实现全自动聚类：
1. **功能码分组**：按 FC01 (Coil), FC02 (Discrete), FC03 (Holding), FC04 (Input) 分流；
2. **地址升序排列**：按寄存器起始偏移排序；
3. **区间动态合并**：设定空洞阈值 `max_gap` 与单包最大寄存器数 `max_regs_per_req`，将相邻测点聚类为一个请求块，**网络往返降低 90%**！

### 2.3 集中控制指令毫秒级优先插队
Modbus TCP 为半双工应答式通道。当调度员在 Web 界面发起写单个寄存器（FC06）或写线圈（FC05）时，FEP 前置机在轮询循环间隙瞬间抢占通信通道，优先执行写指令（实测执行耗时约 0.05ms），执行完毕后立即恢复周期轮询。

---

## 3. 编译与安装指南

### 3.1 环境要求
- **操作系统**：Linux (Ubuntu 20.04+, Debian, CentOS 8+, Alpine) 或 Windows (MSVC 2022)
- **编译器**：支持 C++20 的编译器（推荐 `GCC 11+` / `GCC 12+` 或 `Clang 14+`）
- **依赖库**：`zlib` 与 `sqlite3`（现代 Linux 发行版均自带或一条命令安装）
  ```bash
  # Ubuntu / Debian
  sudo apt install -y g++-12 cmake zlib1g-dev libsqlite3-dev
  ```

### 3.2 编译步骤

```bash
# 1. 克隆代码仓库
git clone https://github.com/alongL/edgeScada.git
cd edgeScada

# 2. 创建编译目录并配置
mkdir build && cd build
cmake .. -DCMAKE_CXX_COMPILER=g++-12

# 3. 编译构建
make -j$(nproc)
```

编译成功后，将在 `build/` 目录下生成：
- `edgescada`：EdgeSCADA 中控调度服务器
- `edgescada-fep`：前置通信采集引擎
- `web/`：静态 Web 监控调度看板
- `config/`：示例配置文件与 CSV 点表模板

---

## 4. 使用指南与命令行参数

### 4.1 快速启动（一键脚本运行）

项目根目录提供了一键联动脚本 [`run.sh`](run.sh)，可同时拉起中控与 FEP 采集机，并管理后台进程生命周期：

```bash
./run.sh
```

- **访问 SCADA 监控调度大屏**：直接打开浏览器访问 `http://localhost:8888`；
- **运行日志归档**：中控日志自动输出到 `logs/scada.log`，前置机日志输出到 `logs/fep.log`；
- **一键退出**：在终端按 `Ctrl + C`，脚本会自动捕获信号并安全停止中控与前置机。

---

### 4.2 独立进程分步启动 (手动模式)

#### 终端 1：启动 SCADA 中控服务
```bash
./build/edgescada --tcp-port 9000 --web-port 8888
```

#### 终端 2：启动 FEP 前置采集引擎
```bash
./build/edgescada-fep --central-host 127.0.0.1 --central-port 9000
```

### 4.2 命令行启动参数

#### `edgescada` (SCADA 中控服务)
| 参数项 | 默认值 | 作用说明 |
| :--- | :--- | :--- |
| `--tcp-port <端口>` | `9000` | 接收 FEP 前置机 CompactPack 帧的南向端口 |
| `--web-port <端口>` | `8888` | 北向 Web 调度大屏与 REST 控制 API 端口 |
| `--web-dir <路径>` | `./web` | 静态 Web 资源根目录路径 |
| `--db <路径>` | `data/scada.db` | SQLite3 历史时序与告警数据库路径 (自动创建目录) |

#### `edgescada-fep` (前置通信采集引擎)
| 参数项 | 默认值 | 作用说明 |
| :--- | :--- | :--- |
| `--central-host <主机>` | `127.0.0.1` | 指定 SCADA 中控服务器 IP 地址或域名 |
| `--central-port <端口>` | `9000` | 指定 SCADA 中控服务器 TCP 接收端口 |
| `--id <标识>` | `collector_01` | 前置机唯一编号 |
| `--no-mock` | 启用仿真 | 禁用内置虚拟仿真桩，连接物理现场 Modbus 设备 |
| `--modbus-port <端口>` | `1502` | 仿真桩或现场物理设备端口 |
| `--web-port <端口>` | `8080` | 前置机本地备用自检 Web 端口 |

---

## 5. 海量测点与设备配置指南 (CSV 模板化架构)

在工业现场（如包含 50 台相同型号逆变器，每台有 1,000 个测点），EdgeSCADA 采用**模板与实例分离架构**：
- **设备模板 (`config/templates/*.csv`)**：直接使用 Excel 编制并导出为 CSV；
- **设备清单 (`config/devices.json`)**：仅需配置设备 IP 与引用的模板名，1 对多秒级复用！

### Excel / CSV 点表模板示例 (`config/templates/inverter_template.csv`)
```csv
# 测点名称, 功能码, 寄存器地址[:位偏移], 数据类型, 比例系数, 偏移量, 工程单位, 描述说明
active_power, 3, 100, float32_cdab, 1.0, 0.0, kW, 机组有功功率
reactive_power, 3, 102, float32_cdab, 1.0, 0.0, kvar, 机组无功功率
grid_voltage_a, 3, 104, float32_cdab, 1.0, 0.0, V, A相电网电压
grid_current_a, 3, 106, float32_cdab, 1.0, 0.0, A, A相电网电流
grid_frequency, 3, 108, float32_cdab, 1.0, 0.0, Hz, 电网工频
inverter_temp, 3, 110, int16, 0.1, 0.0, ℃, 变流器IGBT温度
daily_energy, 3, 112, uint32_abcd, 0.1, 0.0, kWh, 当日累计发电量
running_state, 3, 120:0, boolean, 1.0, 0.0, , 机组运行状态 (第0位)
fault_state, 3, 120:1, boolean, 1.0, 0.0, , 机组综合故障 (第1位)
```

---

## 6. Web 监控大屏与 REST API

中控内置了高并发异步 HTTP 服务器，调度员与外部系统可直接调用：

| API 端点 | HTTP 方法 | 功能说明 |
| :--- | :--- | :--- |
| `GET /api/status` | `GET` | 查询 SCADA 系统总体状态、在线前置机数与设备列表 |
| `GET /api/points` | `GET` | 获取场站内各设备最新测点镜像与质量戳 |
| `GET /api/alarms` | `GET` | 查询最新越限告警与故障事件列表 |
| `GET /api/history` | `GET` | 查询特定设备测点的历史时序回放采样 (`?device=...&point=...`) |
| `POST /api/control` | `POST` | 下发集中控制指令，中控自动路由转发到目标 FEP 并入库审计 |

---

## 7. 开源协议与商业授权声明

本项目采用 **双重许可模式（Dual-Licensing）**：

### 7.1 社区开源版 (GNU GPL v3)
- 本项目遵循 **[GNU General Public License v3.0 (GPLv3)](LICENSE)** 协议开源；
- 允许所有人免费学习、研究、部署与二次开发；
- **开源合规约束**：若对本项目代码进行修改、集成或二次封装，并向客户分发该软件，**必须同样将衍生产品的完整源代码以 GPLv3 协议向公众开源！**

### 7.2 商业与企业级授权 (Commercial License)
- 若您的企业需要将 `EdgeSCADA` 集成至专有闭源商业产品中，豁免 GPLv3 开源约束，请购买商业闭源授权；
- 商业授权用户享有闭源商业权利以及专属技术支持与协议定制开发服务。
