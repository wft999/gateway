/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef MODULE_H
#define MODULE_H


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

#if defined(__GNUC__)
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
  #elif _MSC_VER
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

#define TRUE 1
#define FALSE 0
#define timestamp_t u_int64_t


#ifndef really_inline
#define really_inline __attribute__((always_inline)) inline
#endif // really_inline

extern RedisModuleCtx *rts_staticCtx;

// Calculate the begining of aggregation bucket
static inline timestamp_t CalcBucketStart(timestamp_t ts,
                                          timestamp_t bucketDuration,
                                          timestamp_t timestampAlignment) {
    const int64_t timestamp_diff = ts - timestampAlignment;
    return ts - modulo(timestamp_diff, (int64_t)bucketDuration);
}
static inline timestamp_t BucketStartNormalize(timestamp_t bucketTS) {
    return max(0, (int64_t)bucketTS);
}
#endif
