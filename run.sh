#!/usr/bin/env bash
# ==============================================================================
# EdgeSCADA 一键启动脚本
# 同时拉起 SCADA 中控大脑 (edgescada) 与 FEP 前置采集机 (edgescada-fep)
# 按 Ctrl+C 可一键安全退出所有后台进程
# ==============================================================================

set -e

# 定位脚本所在目录 (无论在哪个路径执行都能正确找到二进制)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${SCRIPT_DIR}/build"

CENTRAL_BIN="${BIN_DIR}/edgescada"
FEP_BIN="${BIN_DIR}/edgescada-fep"

# 检查二进制文件是否存在，不存在则提示编译
if [[ ! -f "${CENTRAL_BIN}" || ! -f "${FEP_BIN}" ]]; then
    echo "❌ 未检测到编译产物，正在为您自动编译..."
    mkdir -p "${BIN_DIR}"
    cd "${BIN_DIR}"
    cmake .. -DCMAKE_CXX_COMPILER=g++-12
    make -j$(nproc)
    cd "${SCRIPT_DIR}"
fi

# 确保数据目录存在
mkdir -p "${SCRIPT_DIR}/data"
mkdir -p "${SCRIPT_DIR}/logs"

echo "======================================================================"
echo "          ⚡ 启动 EdgeSCADA 工业自动化监控与数据采集系统 ⚡"
echo "======================================================================"

# 记录子进程 PID
CENTRAL_PID=""
FEP_PID=""

# 优雅退出信号清理钩子
cleanup() {
    echo ""
    echo "🛑 正在停止 EdgeSCADA 所有组件..."
    if [[ -n "${FEP_PID}" ]] && kill -0 "${FEP_PID}" 2>/dev/null; then
        echo "   -> 停止 FEP 前置采集机 (PID: ${FEP_PID})..."
        kill -SIGTERM "${FEP_PID}" 2>/dev/null || true
    fi
    if [[ -n "${CENTRAL_PID}" ]] && kill -0 "${CENTRAL_PID}" 2>/dev/null; then
        echo "   -> 停止 SCADA 中控服务器 (PID: ${CENTRAL_PID})..."
        kill -SIGTERM "${CENTRAL_PID}" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    echo "✅ 全部进程已安全退出。"
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# 1. 启动 SCADA 中控服务 (监听 9000 端口，Web 面板 8888 端口)
echo "🚀 [1/2] 启动 SCADA 中控调度大脑 (edgescada)..."
"${CENTRAL_BIN}" --tcp-port 9000 --web-port 8888 --web-dir "${SCRIPT_DIR}/web" --db "${SCRIPT_DIR}/data/scada.db" > "${SCRIPT_DIR}/logs/central.log" 2>&1 &
CENTRAL_PID=$!
echo "       -> 中控进程已启动 (PID: ${CENTRAL_PID})"
echo "       -> 日志输出: logs/scada.log"

# 等待 1 秒确保中控 TCP 监听就绪
sleep 1

# 2. 启动 FEP 前置通信采集机 (连接中控 9000 端口)
echo "🚀 [2/2] 启动 FEP 现场前置采集引擎 (edgescada-fep)..."
cd "${SCRIPT_DIR}"
"${FEP_BIN}" --central-host 127.0.0.1 --central-port 9000 --web-port 8081 > "${SCRIPT_DIR}/logs/fep.log" 2>&1 &
FEP_PID=$!
echo "       -> 前置机进程已启动 (PID: ${FEP_PID})"
echo "       -> 日志输出: logs/fep.log"

echo "======================================================================"
echo "🎉 EdgeSCADA 联动运行中！"
echo "   🌐 SCADA 调度监控大屏 : http://localhost:8888"
echo "   🔧 前置机本地自检接口 : http://localhost:8081"
echo "   💾 SQLite 历史时序数据 : data/scada.db"
echo "   👉 按 [Ctrl + C] 可一键停止全部系统组件"
echo "======================================================================"
echo ""

# 持续等待并监控子进程存活状态
while true; do
    if ! kill -0 "${CENTRAL_PID}" 2>/dev/null; then
        echo "⚠️ SCADA 中控服务意外退出！请检查 logs/central.log"
        exit 1
    fi
    if ! kill -0 "${FEP_PID}" 2>/dev/null; then
        echo "⚠️ FEP 前置采集机意外退出！请检查 logs/fep.log"
        exit 1
    fi
    sleep 2
done
