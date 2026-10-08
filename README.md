# EdgeSCADA ⚡

> **Modern Industrial SCADA Suite & High-Performance Edge Engine**  
> An industrial-grade SCADA monitoring, dispatching suite, and high-precision edge collection engine built with pure **modern C++20 Asio coroutines**.  
> Features the SCADA Central Dispatcher Server (`edgescada`), Front-End Processor engine (`edgescada-fep`), CompactPack nested binary protocol (KLV+Gzip), embedded SQLite3 time-series storage, real-time threshold alarm engine, and a dynamic web visualization console.

[![License](https://img.shields.io/badge/License-GPL_v3-orange.svg)](LICENSE)
[![Language](https://img.shields.io/badge/Language-C%2B%2B20-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B20)
[![Asio](https://img.shields.io/badge/Asio-Standalone-green.svg)](https://think-async.com/Asio/)
[![Storage](https://img.shields.io/badge/Storage-SQLite3%20WAL-lightgrey.svg)](https://sqlite.org/)
[![Chinese](https://img.shields.io/badge/Language-中文说明-red.svg)](README_CN.md)

[English Documentation](README.md) | [中文说明文档](README_CN.md)

---

## 📖 Table of Contents

- [1. Overview & Architecture](#1-overview--architecture)
  - [1.1 Layered Architecture](#11-layered-architecture)
  - [1.2 SCADA Central Server (`edgescada`)](#12-scada-central-server-edgescada)
  - [1.3 Front-End Processor (`edgescada-fep`)](#13-front-end-processor-edgescada-fep)
  - [1.4 CompactPack Wire Protocol (KLV + Gzip)](#14-compactpack-wire-protocol-klv--gzip)
- [2. Key Technologies](#2-key-technologies)
  - [2.1 Sub-Millisecond Precision Pacer](#21-sub-millisecond-precision-pacer)
  - [2.2 AutoCoalesce Modbus Algorithm](#22-autocoalesce-modbus-algorithm)
  - [2.3 Priority Control Command Interleaving](#23-priority-control-command-interleaving)
  - [2.4 Real-time Alarm Engine & SQLite3 Archival](#24-real-time-alarm-engine--sqlite3-archival)
- [3. Build & Installation Guide](#3-build--installation-guide)
- [4. Usage & Command-Line Arguments](#4-usage--command-line-arguments)
  - [4.1 One-Click Startup (`run.sh`)](#41-one-click-startup-runsh)
  - [4.2 Manual Launch](#42-manual-launch)
  - [4.3 CLI Parameter Matrix](#43-cli-parameter-matrix)
- [5. Template-Instance Configuration Guide](#5-template-instance-configuration-guide)
- [6. Web Consoles & REST APIs](#6-web-consoles--rest-apis)
- [7. Dual-Licensing & Commercial Support](#7-dual-licensing--commercial-support)

---

## 1. Overview & Architecture

Legacy SCADA systems and DCS field controllers in industrial automation and renewable energy (wind, solar, BESS, microgrids) often suffer from:
1. **Bloated Software Footprints**: Bulky runtime dependencies taking tens of gigabytes, impractical for resource-constrained edge gateways.
2. **Cumulative Timing Jitter**: Simple `sleep(1000)` polling mechanisms cause network and processing delays to compound, destroying multi-unit time-alignment.
3. **Verbose Wire Protocols**: Heavy text formats like JSON waste valuable edge bandwidth and consume excessive CPU cycles on serialization.

**EdgeSCADA** re-engineers the industrial stack using pure C++20 coroutines into a cohesive two-tier topology: the **Front-End Processor (FEP)** and the **Central SCADA Server**.

### 1.1 Layered Architecture

```
┌────────────────────────────────────────────────────────┐
│            Field Industrial Equipment (PLC/Inverter)   │
└───────────────────────────┬────────────────────────────┘
                            │ Modbus TCP (Standard 502 / Custom Port)
                            ▼
┌────────────────────────────────────────────────────────┐
│              edgescada-fep (Front-End Processor)       │
│                                                        │
│  ┌───────────────────────┐   ┌───────────────────────┐ │
│  │ PrecisionPacer Engine │   │ AutoCoalesce Batching │ │
│  │ (<0.1ms Jitter Lock)  │   │ (Cluster-based Reads) │ │
│  └───────────────────────┘   └───────────────────────┘ │
│  ┌───────────────────────┐   ┌───────────────────────┐ │
│  │ Modbus Coroutine IO   │   │ Priority Queue        │ │
│  │ (Asynchronous Socket) │   │ (Sub-ms Interleaving) │ │
│  └───────────────────────┘   └───────────────────────┘ │
└───────────────────────────┬────────────────────────────┘
                            │ CompactPack TCP Stream (KLV + Transparent Gzip)
                            ▼
┌────────────────────────────────────────────────────────┐
│              edgescada (Central SCADA Server)          │
│                                                        │
│  ┌────────────────────────┐  ┌───────────────────────┐ │
│  │ Live Telemetry Mirror  │  │ Real-time Alarm Engine│ │
│  └────────────────────────┘  └───────────────────────┘ │
│  ┌────────────────────────┐  ┌───────────────────────┐ │
│  │ SQLite3 WAL Archival   │  │ Control Dispatcher    │ │
│  └────────────────────────┘  └───────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  High-Concurrency Async Web Dashboard (Port 8888)  │ │
│  └───────────────────────────────────────────────────┘ │
└───────────────────────────┬────────────────────────────┘
                            │ HTTP / REST / Dynamic Web UI
                            ▼
                 [ Operator Central Console / SCADA Screen ]
```

### 1.2 SCADA Central Server (`edgescada`)
- **Connection Management**: Maintains upstream persistent sessions and handshakes from field FEP nodes.
- **Real-Time In-Memory Mirror**: Maintains an ultra-fast, zero-allocation memory mirror of all live metrics across the entire plant.
- **Threshold & Fault Alarm Engine**: Real-time evaluation of telemetry points with configurable thresholds (WARN / CRITICAL) and 10-second anti-flapping filters.
- **SQLite3 WAL Storage**: Embedded time-series history logging, alarm tracking, and control audit trails stored cleanly in `data/scada.db`.
- **SCADA Web Dispatcher**: Out-of-the-box dynamic visualization console with equipment power indicators, alarms, and control routing.

### 1.3 Front-End Processor (`edgescada-fep`)
- Sits close to physical field gear inside industrial switchgear cabinets.
- Delivers deterministic polling without exposing vulnerable business ports to untrusted networks.
- Features offline buffering and local bus diagnostic terminal on port `8081`.

### 1.4 CompactPack Wire Protocol (KLV + Gzip)
Eliminates JSON parsing overhead and bracket noise:
- Structured byte stream: `[uint16 key_len][key][uint32 val_len][val]`.
- **Pack-in-Pack Nesting**: Sub-packs (e.g. an array of 50 data points) serialize directly as binary values into the parent packet.
- **Transparent Gzip Compression**: Payloads over 64 bytes are hardware-compressed using zlib deflate, slashing bandwidth consumption.

---

## 2. Key Technologies

### 2.1 Sub-Millisecond Precision Pacer
Traditional periodic polling accumulates processing delay $T_{exec}$ at each loop iteration.  
EdgeSCADA utilizes a **two-phase deterministic timing engine**:
1. **Wall-Clock Alignment**: Calculates the nanosecond difference to the nearest second boundary (`:00.000`) and waits for exact triggering.
2. **Monotonic Absolute Pacing**: Advances on `std::chrono::steady_clock` by constant period increments:
   $$\text{Next Deadline} = \text{Next Deadline} + \Delta t$$
   Even under network jitter, it eliminates drift, maintaining jitter under **$<0.1\,\text{ms}$** (typically $50\sim 80\,\mu\text{s}$).

### 2.2 AutoCoalesce Modbus Algorithm
For hundreds of discrete telemetry points, manual block packing is error-prone. `AutoCoalesce` automates clustering:
1. Groups points by Modbus function codes (FC01, FC02, FC03, FC04).
2. Sorts points by register offset.
3. Merges contiguous addresses within `max_gap` threshold up to `max_regs_per_req`, **reducing Modbus round-trips by up to 90%**.

### 2.3 Priority Control Command Interleaving
Modbus TCP operates in half-duplex request-response mode. When operators issue write commands (FC05/FC06) via the web console, the FEP immediately preempts the poll cycle, executes the write within $\approx 0.05\,\text{ms}$, and resumes cyclic reading seamlessly.

---

## 3. Build & Installation Guide

### 3.1 Requirements
- **OS**: Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+, Alpine) or Windows (MSVC 2022).
- **Compiler**: C++20 compliant compiler (`GCC 11+` / `GCC 12+` or `Clang 14+`).
- **Dependencies**: `zlib` and `sqlite3` development libraries.
  ```bash
  # Debian / Ubuntu
  sudo apt install -y g++-12 cmake zlib1g-dev libsqlite3-dev
  ```

### 3.2 Compilation

```bash
# 1. Clone repository
git clone https://github.com/alongL/edgeScada.git
cd edgeScada

# 2. Configure build
mkdir build && cd build
cmake .. -DCMAKE_CXX_COMPILER=g++-12

# 3. Compile
make -j$(nproc)
```

Binaries generated in `build/`:
- `edgescada`: SCADA Central Dispatcher Server
- `edgescada-fep`: Front-End Processor Engine

---

## 4. Usage & Command-Line Arguments

### 4.1 One-Click Startup (`run.sh`)

An automated supervisor script is included in the root directory:

```bash
./run.sh
```

- **Open SCADA Dashboard**: Navigate your browser to `http://localhost:8888`.
- **Log Files**: Automatic logging to `logs/scada.log` and `logs/fep.log`.
- **Graceful Shutdown**: Press `Ctrl + C` to safely terminate all child processes and commit SQLite transactions.

### 4.2 Manual Launch

#### Terminal 1: Launch SCADA Central Server
```bash
./build/edgescada --tcp-port 9000 --web-port 8888
```

#### Terminal 2: Launch FEP Edge Collector
```bash
./build/edgescada-fep --central-host 127.0.0.1 --central-port 9000
```

### 4.3 CLI Parameter Matrix

#### `edgescada` (SCADA Server)
| Flag | Default | Description |
| :--- | :--- | :--- |
| `--tcp-port <port>` | `9000` | Southbound port accepting CompactPack connections from FEPs |
| `--web-port <port>` | `8888` | Northbound port for the operator Web UI & REST APIs |
| `--web-dir <path>` | `./web` | Root directory for static web assets |
| `--db <path>` | `data/scada.db`| SQLite database path for history and alarm storage |

#### `edgescada-fep` (FEP Engine)
| Flag | Default | Description |
| :--- | :--- | :--- |
| `--central-host <host>` | `127.0.0.1` | IP or hostname of the Central SCADA server |
| `--central-port <port>` | `9000` | TCP port of the Central SCADA server |
| `--id <str>` | `collector_01` | Unique identifier for this FEP node |
| `--no-mock` | Enabled | Disable internal simulator to connect to physical Modbus gear |
| `--modbus-port <port>` | `1502` | Port of target Modbus TCP device |
| `--web-port <port>` | `8080` | Local on-site diagnostic web port |

---

## 5. Template-Instance Configuration Guide

EdgeSCADA decouples device models from physical communication instances:
- **Device Template (`config/templates/*.csv`)**: Define tag names, offsets, data types, and scale factors once in Excel and export to CSV.
- **Instance Manifest (`config/devices.json`)**: Map device addresses to shared templates.

### Excel / CSV Template (`config/templates/inverter_template.csv`)
```csv
# Tag Name, Function Code, Register Offset[:Bit Offset], Data Type, Scale, Bias, Unit, Description
active_power, 3, 100, float32_cdab, 1.0, 0.0, kW, Inverter Active Power
reactive_power, 3, 102, float32_cdab, 1.0, 0.0, kvar, Reactive Power
grid_voltage_a, 3, 104, float32_cdab, 1.0, 0.0, V, Phase A Grid Voltage
grid_current_a, 3, 106, float32_cdab, 1.0, 0.0, A, Phase A Grid Current
grid_frequency, 3, 108, float32_cdab, 1.0, 0.0, Hz, Grid Frequency
inverter_temp, 3, 110, int16, 0.1, 0.0, ℃, Heat Sink Temperature
daily_energy, 3, 112, uint32_abcd, 0.1, 0.0, kWh, Cumulative Daily Energy
running_state, 3, 120:0, boolean, 1.0, 0.0, , Running Status Bit
fault_state, 3, 120:1, boolean, 1.0, 0.0, , Fault Status Bit
```

---

## 6. Web Consoles & REST APIs

### 6.1 Two-Tier Responsive Web Consoles
1. **SCADA Central Console (`:8888`)**:
   - Equipment power matrix visualization.
   - Click-to-drill-down telemetry table per device.
   - Real-time alarms center and SQLite time-series replay.
   - Remote write command dispatching.
2. **FEP Local Diagnostic Console (`:8081`)**:
   - Sub-millisecond timing jitter monitor.
   - AutoCoalesce merge statistics and bus round-trip latency.

### 6.2 REST APIs

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `GET /api/status` | `GET` | Retrieve overall cluster metrics and active FEP nodes |
| `GET /api/points` | `GET` | Query latest point mirror and quality stamps |
| `GET /api/alarms` | `GET` | List active and historical threshold alarms |
| `GET /api/history`| `GET` | Query time-series telemetry (`?device=...&point=...`) |
| `POST /api/control`| `POST`| Dispatch setpoint command with audit logging |

---

## 7. Dual-Licensing & Commercial Support

EdgeSCADA is distributed under a **Dual-Licensing model**:

### 7.1 Community Open-Source Edition (GNU GPLv3)
- Licensed under the **[GNU General Public License v3.0 (GPLv3)](LICENSE)**.
- Free for evaluation, research, and non-commercial development.
- **Copyleft Notice**: If you modify, embed, or distribute EdgeSCADA as part of a product, you must provide full source code under GPLv3.

### 7.2 Commercial Proprietary License
- For enterprise organizations looking to:
  1. Embed EdgeSCADA into closed-source commercial software or hardware gateways.
  2. Waive GPLv3 copyleft open-source obligations.
  3. Acquire enterprise SLA, custom protocol integrations (e.g. IEC 60870-5-104, OPC UA), and dedicated engineering support.
- **Please inquire for a Commercial License agreement.**
