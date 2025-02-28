#!/bin/bash

mkdir ../../db/snapshots
tarantool ./migrations/migrate_to_v1.lua