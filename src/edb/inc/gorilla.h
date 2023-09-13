/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef GORILLA_H
#define GORILLA_H

#include <stdbool.h>   // bool
#include <sys/types.h> // u_int_t

  #if defined(__GNUC__)
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
  #elif _MSC_VER
#define likely(x)       (x)
#define unlikely(x)     (x)
  #endif

typedef u_int64_t binary_t;
typedef u_int64_t globalbit_t;
typedef u_int8_t localbit_t;

#define CHUNK_RESIZE_STEP 128

typedef void Chunk_t;
typedef void ChunkIter_t;

/* Chunk enum */
typedef enum {
  CR_OK = 0,    // RM_OK
  CR_ERR = 1,   // RM_ERR
  CR_END = 2,   // END_OF_CHUNK
} ChunkResult;

typedef union
{
    double d;
    int64_t i;
    u_int64_t u;
} union64bits;
#pragma pack(1)
typedef struct CompressedChunk
{
    u_int64_t size;
    u_int64_t count;
    u_int64_t idx;

    union64bits baseValue;
    union64bits prevValue;
    u_int8_t prevLeading;
    u_int8_t prevTrailing;
    u_int64_t data[0];
} CompressedChunk;
#pragma pack()
typedef struct Compressed_Iterator
{
    CompressedChunk *chunk;
    u_int64_t idx;
    u_int64_t count;

    // value vars
    union64bits prevValue;
    u_int8_t leading;
    u_int8_t trailing;
    u_int8_t blocksize;
} Compressed_Iterator;

ChunkResult Compressed_Append(CompressedChunk *chunk, double value);
ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, double *sample);
void Compressed_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk);
ChunkIter_t *Compressed_NewChunkIterator(const Chunk_t *chunk);
void Compressed_FreeChunkIterator(ChunkIter_t *iter);

#endif
