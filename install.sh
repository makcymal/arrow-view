#!/bin/bash

SOURCE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cmake -S $SOURCE_DIR -B .cmake -Wno-dev

make -C .cmake arrow-head arrow-info arrow-desc

BUILD_DIR=$SOURCE_DIR/.build

INSTALL_DIR=/usr/local/bin

sudo cp $BUILD_DIR/arrow-head $INSTALL_DIR/arrow-head
sudo cp $BUILD_DIR/arrow-info $INSTALL_DIR/arrow-info
sudo cp $BUILD_DIR/arrow-desc $INSTALL_DIR/arrow-desc
