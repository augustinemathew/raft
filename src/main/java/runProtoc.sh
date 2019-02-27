#!/bin/bash
export SOURCE_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
pushd ${SOURCE_DIR}
protoc --proto_path=${SOURCE_DIR} \
  --java_out=. \
  `pwd`/com/augustine/raft/wal/proto/wal.proto

popd

