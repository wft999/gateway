/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "redismodule.h"
#include "tsdb.h"

#ifndef RDB_H
#define RDB_H
#define TS_ENC_VER 0

// This flag should be updated whenever a new rdb version is introduced
#define TS_LATEST_ENCVER TS_ENC_VER

void DefaultAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
void *device_rdb_load(RedisModuleIO *io, int encver);
void device_rdb_save(RedisModuleIO *io, void *value);

#endif
