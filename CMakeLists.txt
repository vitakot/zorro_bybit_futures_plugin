cmake_minimum_required(VERSION 3.30)
project(bybit_futures_zorro_plugin)

set(CMAKE_CXX_FLAGS_RELEASE "/Ox")

set(CMAKE_CXX_STANDARD 20)
add_definitions(-D_WIN32_WINNT=0x0A00 /utf-8)

find_package(spdlog CONFIG REQUIRED)

include_directories(include bybit_cpp_api/include bybit_cpp_api/vk_cpp_common/include)
add_subdirectory(bybit_cpp_api)

add_library(BybitFutures SHARED dllmain.cpp src/bybit_futures.cpp src/stdafx.cpp)
target_compile_definitions(BybitFutures PUBLIC BYBIT_FUTURES_DLL_EXPORTS)

target_link_libraries(BybitFutures PRIVATE spdlog::spdlog_header_only bybit_api)
