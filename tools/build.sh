#!/bin/env bash
# This script builds the project in debug mode using clang-18.

# Parse arguments -----------------------------------------------------------------------------------+
# Convert first arg to all caps
BUILD_MODE=$(echo "$1" | tr '[:lower:]' '[:upper:]')
echo "Building in $BUILD_MODE mode..."
# Ensure build mode is valid
if [[ "$BUILD_MODE" != "DEBUG" && "$BUILD_MODE" != "RELEASE" ]]; then
	echo "Usage: $0 <debug|release>"
	exit 1
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
	-DBUILD_MODE="$BUILD_MODE" ..
# -DCMAKE_VERBOSE_MAKEFILE=ON
# Compile
make -j$(nproc)
# Go back to root
cd $root
