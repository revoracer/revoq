cmake_minimum_required(VERSION 3.23)

project(
    revoq
    VERSION 0.1.0
    LANGUAGES CXX
)

option(REVOQ_BUILD_TESTS "Build revoq C++ tests" ON)
option(REVOQ_BUILD_BENCHMARKS "Build revoq benchmarks" OFF)
option(REVOQ_BUILD_EXAMPLES "Build revoq examples" ON)
option(REVOQ_BUILD_PYTHON "Build revoq Python bindings" OFF)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/fmt/CMakeLists.txt")
    message(FATAL_ERROR "Missing external/fmt. Run: git submodule update --init --recursive")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/spdlog/CMakeLists.txt")
    message(FATAL_ERROR "Missing external/spdlog. Run: git submodule update --init --recursive")
endif()


set(FMT_DOC OFF CACHE BOOL "" FORCE)
set(FMT_TEST OFF CACHE BOOL "" FORCE)
set(FMT_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(external/fmt EXCLUDE_FROM_ALL)


set(SPDLOG_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS_HO OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)
set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
add_subdirectory(external/spdlog EXCLUDE_FROM_ALL)

add_library(revoq)
add_library(revoq::revoq ALIAS revoq)

file(GLOB_RECURSE REVOQ_PUBLIC_HEADERS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/include/revoq/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/revoq/*.hpp"
)

file(GLOB_RECURSE REVOQ_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cxx"
)

target_sources(revoq
    PRIVATE
        ${REVOQ_SOURCES}
    PUBLIC
        FILE_SET HEADERS
        BASE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include"
        FILES ${REVOQ_PUBLIC_HEADERS}
)

target_include_directories(revoq
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(revoq
    PUBLIC
        fmt::fmt
        spdlog::spdlog
        Threads::Threads
)

if(REVOQ_BUILD_TESTS)
    include(CTest)
    if(BUILD_TESTING)
        if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/Catch2/CMakeLists.txt")
            message(FATAL_ERROR "Missing external/Catch2. Run: git submodule update --init --recursive")
        endif()

        set(CATCH_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
        set(CATCH_INSTALL_EXTRAS OFF CACHE BOOL "" FORCE)
        set(CATCH_BUILD_TESTING OFF CACHE BOOL "" FORCE)
        add_subdirectory(external/Catch2 EXCLUDE_FROM_ALL)
        list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/external/Catch2/extras")

        file(GLOB REVOQ_TEST_SOURCES CONFIGURE_DEPENDS
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
        )

        if(REVOQ_TEST_SOURCES)
            add_executable(revoq_tests ${REVOQ_TEST_SOURCES})
            target_link_libraries(revoq_tests
                PRIVATE
                    revoq::revoq
                    Catch2::Catch2WithMain
            )
            target_include_directories(revoq_tests
                PRIVATE
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests"
            )

            include(Catch)
            catch_discover_tests(revoq_tests)
        endif()
    endif()
endif()

if(REVOQ_BUILD_EXAMPLES)
    file(GLOB REVOQ_EXAMPLE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/examples/*.cpp"
    )

    foreach(example_source IN LISTS REVOQ_EXAMPLE_SOURCES)
        get_filename_component(example_name "${example_source}" NAME_WE)
        add_executable("revoq_example_${example_name}" "${example_source}")
        target_link_libraries("revoq_example_${example_name}" PRIVATE revoq::revoq)
    endforeach()
endif()

if(REVOQ_BUILD_BENCHMARKS)
    # HdrHistogram-C provides the lossless, industry-standard latency histogram
    # encoding (base64 "V2") that the p99 benchmark emits, so its output is
    # directly comparable with Chronicle/Aeron in benchmarks/comparative/.
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/HdrHistogram_c/CMakeLists.txt")
        message(FATAL_ERROR "Missing external/HdrHistogram_c. Run: git submodule update --init --recursive")
    endif()
    set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(HDR_HISTOGRAM_INSTALL_SHARED OFF CACHE BOOL "" FORCE)
    set(HDR_HISTOGRAM_INSTALL_STATIC OFF CACHE BOOL "" FORCE)
    add_subdirectory(external/HdrHistogram_c EXCLUDE_FROM_ALL)

    # Only the revoq-only micro/regression benchmarks are part of the core build.
    # benchmarks/comparative/ carries cross-system (JVM) drivers with their own
    # toolchains and is intentionally excluded from CMake.
    file(GLOB REVOQ_BENCHMARK_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/micro/*.cpp"
    )

    foreach(benchmark_source IN LISTS REVOQ_BENCHMARK_SOURCES)
        get_filename_component(benchmark_name "${benchmark_source}" NAME_WE)
        add_executable("revoq_benchmark_${benchmark_name}" "${benchmark_source}")
        target_link_libraries("revoq_benchmark_${benchmark_name}" PRIVATE revoq::revoq)
        target_include_directories("revoq_benchmark_${benchmark_name}"
            PRIVATE
                "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/micro"
        )
    endforeach()

    # Only the latency benchmark records an HdrHistogram; keep the dependency off
    # the throughput binaries.
    if(TARGET revoq_benchmark_stream_handler_benchmark_p99)
        target_link_libraries(revoq_benchmark_stream_handler_benchmark_p99
            PRIVATE hdr_histogram_static)
    endif()
endif()

if(REVOQ_BUILD_PYTHON)
    find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/pybind11/CMakeLists.txt")
        message(FATAL_ERROR "Missing external/pybind11. Run: git submodule update --init --recursive")
    endif()

    set(PYBIND11_INSTALL OFF CACHE BOOL "" FORCE)
    set(PYBIND11_TEST OFF CACHE BOOL "" FORCE)
    add_subdirectory(external/pybind11 EXCLUDE_FROM_ALL)

    pybind11_add_module(revoq_py MODULE python/revoq_py.cpp)
    target_link_libraries(revoq_py PRIVATE revoq::revoq)
    set_target_properties(revoq_py PROPERTIES OUTPUT_NAME revoq)
endif()
