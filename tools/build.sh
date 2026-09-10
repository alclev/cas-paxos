#!/bin/env bash
# This script builds the project in debug mode using clang-18.

function usage() {
    echo "Usage: $0 <debug|release> <musq|vesq>"
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

if [[ "$MODE" != "MUSQ" && "$MODE" != "VESQ" ]]; then
    usage
fi


CONDITIONAL_ARGS="-DBUILD_MODE=${BUILD_MODE}"
# If the second arg exists
if [[ "$MODE" == "MUSQ" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DMU_SQUARED=ON -DVELOS_SQUARED=OFF"
elif [[ "$MODE" == "MUSQ" ]]; then
    CONDITIONAL_ARGS="${CONDITIONAL_ARGS} -DMU_SQUARED=OFF -DVELOS_SQUARED=ON"
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
