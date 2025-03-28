#! /usr/bin/env bash

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${HERE}"
go run main.go; popd
