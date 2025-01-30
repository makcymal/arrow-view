#!/bin/bash

SOURCE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cmake -S $SOURCE_DIR -B .cmake -Wno-dev

cmake --build .cmake

BUILD_DIR=$SOURCE_DIR/.build

INSTALL_DIR=/usr/local/bin

if [[ $EUID == 0 ]]; then
  cp $BUILD_DIR/arrow-head $INSTALL_DIR/arrow-head
  cp $BUILD_DIR/arrow-info $INSTALL_DIR/arrow-info
  cp $BUILD_DIR/arrow-desc $INSTALL_DIR/arrow-desc
else
  sudo cp $BUILD_DIR/arrow-head $INSTALL_DIR/arrow-head
  sudo cp $BUILD_DIR/arrow-info $INSTALL_DIR/arrow-info
  sudo cp $BUILD_DIR/arrow-desc $INSTALL_DIR/arrow-desc
fi

echo
echo Binaries placed in /usr/local/bin/
