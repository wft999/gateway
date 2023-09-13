/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef MODULE_H
#define MODULE_H

#include "config.h"
#include "redismodule.h"

#define MAX_STREAM_LENGTH 1024
#define min(a,b) (((a)<(b))?(a):(b))
#define DC 0
#define max(a,b) (((a)>(b))?(a):(b))
#define modulo(x, N) ((x % N + N) % N)
#define __SWAP(x,y) do {  \
  typeof(x) _x = x;      \
  typeof(y) _y = y;      \
  x = _y;                \
  y = _x;                \
} while(0)


#define TRUE 1
#define FALSE 0
#define timestamp_t u_int64_t
#define api_timestamp_t u_int64_t
#define TSDB_OK 0
#define TSDB_ERROR -1
#define TSDB_NOTEXISTS 2


#define CHECK_NODE_PERIOD_MS	1000

#ifndef really_inline
#define really_inline __attribute__((always_inline)) inline
#endif // really_inline

extern RedisModuleCtx *rts_staticCtx;


#endif
