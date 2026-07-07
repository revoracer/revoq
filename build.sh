#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
BUILD_TYPE="Release"
BUILD_TESTS="ON"
BUILD_EXAMPLES="ON"
BUILD_BENCHMARKS="OFF"
BUILD_PYTHON="OFF"
RUN_TESTS="OFF"
RUN_PYTHON_TESTS="OFF"
CONFIGURE_ONLY="OFF"
JOBS=""

usage() {
  cat <<EOF
Usage: ./build.sh [command] [options]

Commands:
  release          Configure and build Release (default)
  debug            Configure and build Debug
  test             Build and run C++ tests
  benchmarks       Build benchmark targets
  examples         Build examples
  python           Build Python bindings and run Python tests
  clean            Remove the build directory
  doctor           Print local build tool versions

Options:
  --build-dir DIR      Use a custom build directory
  -j, --jobs N         Parallel build jobs
  --no-tests          Do not build C++ tests
  --no-examples       Do not build examples
  --configure-only    Configure without building
  -h, --help          Show this help

Examples:
  ./build.sh
  CXX=/usr/bin/clang++-20 ./build.sh benchmarks --jobs 16
  CXX=/usr/bin/clang++-20 ./build.sh python
EOF
}

command="release"
if [[ $# -gt 0 && "$1" != -* ]]; then
  command="$1"
  shift
fi

case "${command}" in
  release)
    BUILD_TYPE="Release"
    ;;
  debug)
    BUILD_TYPE="Debug"
    ;;
  test)
    BUILD_TYPE="Release"
    BUILD_TESTS="ON"
    RUN_TESTS="ON"
    ;;
  benchmarks)
    BUILD_TYPE="Release"
    BUILD_TESTS="OFF"
    BUILD_EXAMPLES="OFF"
    BUILD_BENCHMARKS="ON"
    ;;
  examples)
    BUILD_TYPE="Release"
    BUILD_TESTS="OFF"
    BUILD_EXAMPLES="ON"
    ;;
  python)
    BUILD_TYPE="Release"
    BUILD_TESTS="OFF"
    BUILD_EXAMPLES="OFF"
    BUILD_PYTHON="ON"
    RUN_PYTHON_TESTS="ON"
    ;;
  clean)
    rm -rf "${BUILD_DIR}"
    exit 0
    ;;
  doctor)
    echo "CXX=${CXX:-}"
    cmake --version
    git --version
    if command -v "${CXX:-c++}" >/dev/null 2>&1; then
      "${CXX:-c++}" --version | head -n 1
    else
      echo "C++ compiler not found. Set CXX=/path/to/clang++ or install a C++23 compiler."
    fi
    exit 0
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown command: ${command}" >&2
    usage >&2
    exit 2
    ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      if [[ $# -lt 2 ]]; then
        echo "--build-dir requires a value" >&2
        exit 2
      fi
      BUILD_DIR="$2"
      shift 2
      ;;
    --build-dir=*)
      BUILD_DIR="${1#*=}"
      shift
      ;;
    -j|--jobs)
      if [[ $# -lt 2 ]]; then
        echo "$1 requires a value" >&2
        exit 2
      fi
      JOBS="$2"
      shift 2
      ;;
    --jobs=*)
      JOBS="${1#*=}"
      shift
      ;;
    --no-tests)
      BUILD_TESTS="OFF"
      shift
      ;;
    --no-examples)
      BUILD_EXAMPLES="OFF"
      shift
      ;;
    --configure-only)
      CONFIGURE_ONLY="ON"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cd "${ROOT_DIR}"

mkdir -p external
git submodule update --init --recursive \
  external/fmt \
  external/spdlog \
  external/Catch2 \
  external/pybind11

cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DREVOQ_BUILD_TESTS="${BUILD_TESTS}" \
  -DREVOQ_BUILD_EXAMPLES="${BUILD_EXAMPLES}" \
  -DREVOQ_BUILD_BENCHMARKS="${BUILD_BENCHMARKS}" \
  -DREVOQ_BUILD_PYTHON="${BUILD_PYTHON}"

if [[ "${CONFIGURE_ONLY}" == "ON" ]]; then
  exit 0
fi

build_args=("--build" "${BUILD_DIR}")
if [[ -n "${JOBS}" ]]; then
  build_args+=("--parallel" "${JOBS}")
fi
cmake "${build_args[@]}"

if [[ "${RUN_TESTS}" == "ON" ]]; then
  ctest --test-dir "${BUILD_DIR}" --output-on-failure
fi

if [[ "${RUN_PYTHON_TESTS}" == "ON" ]]; then
  PYTHON_BIN="${PYTHON:-python3}"
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "Python interpreter not found. Set PYTHON=/path/to/python3." >&2
    exit 1
  fi
  if ! "${PYTHON_BIN}" -m pytest --version >/dev/null 2>&1; then
    echo "pytest not found. Install it with: ${PYTHON_BIN} -m pip install pytest" >&2
    exit 1
  fi
  PYTHONPATH="${BUILD_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
    "${PYTHON_BIN}" -m pytest "${ROOT_DIR}/python/tests"
fi
