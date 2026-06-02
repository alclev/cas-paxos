#!/bin/env bash
# This script builds the project in debug mode using clang-18.

function usage() {
    echo "Usage: $0 <debug|release> <default|mu|lease|velos>"
    exit 1
}

# Parse arguments -----------------------------------------------------------------------------------+
if [[ "$#" -ne 1 && "$#" -ne 2 ]]; then
	usage
	exit 1
fi

# Convert first arg to all caps
BUILD_MODE=$(echo "$1" | tr '[:lower:]' '[:upper:]')
MODE=$(echo "$2" | tr '[:lower:]' '[:upper:]')

echo "Building in $BUILD_MODE mode..."
# Ensure build mode is valid
if [[ "$BUILD_MODE" != "DEBUG" && "$BUILD_MODE" != "RELEASE" ]]; then
	usage
fi

if [[ "$MODE" != "DEFAULT" && "$MODE" != "MU" && "$MODE" != "LEASE" && "$MODE" != "VELOS" ]]; then
    usage
fi


CONDITIONAL_ARGS="-DBUILD_MODE=${BUILD_MODE}"
# If the second arg exists
if [[ "$MODE" == "MU" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DUSE_MU=ON -DUSE_VELOS=OFF -DUSE_LEASE=OFF"
elif [[ "$MODE" == "LEASE" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DUSE_MU=OFF -DUSE_VELOS=OFF -DUSE_LEASE=ON"
elif [[ "$MODE" == "DEFAULT" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DUSE_MU=OFF -DUSE_VELOS=OFF -DUSE_LEASE=OFF"
elif [[ "$MODE" == "VELOS" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DUSE_MU=OFF -DUSE_VELOS=ON -DUSE_LEASE=OFF"
fi

# Go into root dir
root=$(git rev-parse --show-toplevel)
cd $root
rm -rf build
mkdir build
cd build
# Flags to cmake
CC=clang-18 CXX=clang++-18 VERBOSE=1 cmake \
	-DCMAKE_PREFIX_PATH=/opt/romulus/lib/cmake \
	-DCMAKE_MODULE_PATH=/opt/romulus/lib/cmake \
	-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	$CONDITIONAL_ARGS ..
# -DCMAKE_VERBOSE_MAKEFILE=ON
# Compile
make -j$(nproc)
# Go back to root
cd $root
