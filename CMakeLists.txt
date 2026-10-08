cmake_minimum_required(VERSION 3.16)
project(EdgeSCADA VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DASIO_STANDALONE)

find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(SQLite3 REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/asio/include
    ${ZLIB_INCLUDE_DIRS}
    ${SQLite3_INCLUDE_DIRS}
)

# 1. EdgeSCADA Server (SCADA 中控大脑主服务)
add_executable(edgescada
    scada/main.cpp
)

target_link_libraries(edgescada PRIVATE
    Threads::Threads
    ZLIB::ZLIB
    SQLite::SQLite3
)

# 2. EdgeSCADA FEP (前置机高精度采集引擎)
add_executable(edgescada-fep
    fep/main.cpp
)

target_link_libraries(edgescada-fep PRIVATE
    Threads::Threads
    ZLIB::ZLIB
)

if (WIN32)
    target_link_libraries(edgescada PRIVATE ws2_32 wsock32)
    target_link_libraries(edgescada-fep PRIVATE ws2_32 wsock32)
endif()

# 拷贝 Web 前端看板与配置文件到构建目录
add_custom_command(TARGET edgescada POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/web
        $<TARGET_FILE_DIR:edgescada>/web
    COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/config
        $<TARGET_FILE_DIR:edgescada>/config
)
