/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_TSCOMPRESSION_H
#define TDENGINE_TSCOMPRESSION_H

#ifdef __cplusplus
extern "C" {
#endif

//#include "tsdb.h"
#define TSDB_DATA_TYPE_BOOL       1       // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2       // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3       // 2 bytes
#define TSDB_DATA_TYPE_INT        4       // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5       // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6       // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7       // 8 bytes
#define TSDB_DATA_TYPE_BINARY     8       // string
#define TSDB_DATA_TYPE_TIMESTAMP  9       // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10      // wide string

#define CHAR_BYTES   sizeof(char)
#define SHORT_BYTES  sizeof(short)
#define INT_BYTES    sizeof(int)
#define LONG_BYTES   sizeof(int64_t)
#define FLOAT_BYTES  sizeof(float)
#define DOUBLE_BYTES sizeof(double)

#define TSDB_DATA_BOOL_NULL       0x02
#define TSDB_DATA_TINYINT_NULL    0x80
#define TSDB_DATA_SMALLINT_NULL   0x8000
#define TSDB_DATA_INT_NULL        0x80000000
#define TSDB_DATA_BIGINT_NULL     0x8000000000000000L

#define TSDB_DATA_FLOAT_NULL      0x7FF00000            // it is an NAN
#define TSDB_DATA_DOUBLE_NULL     0x7FFFFF0000000000L  // an NAN
#define TSDB_DATA_NCHAR_NULL      0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL     0xFF

#define TSDB_DATA_NULL_STR        "NULL"
#define TSDB_DATA_NULL_STR_L      "null"
//////////////////////////////////////

#define BITS_PER_BYTE 8
// Masks
#define INT64MASK(_x) ((1ul << _x) - 1)
#define INT32MASK(_x) (((uint32_t)1 << _x) - 1)
#define INT8MASK(_x) (((uint8_t)1 << _x) - 1)
// Compression algorithm
#define NO_COMPRESSION 0
#define ONE_STAGE_COMP 1
#define TWO_STAGE_COMP 2

int tsCompressTinyint(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorithm,
                      char* const buffer, int bufferSize);
int tsCompressSmallint(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                       char* const buffer, int bufferSize);
int tsCompressInt(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                  char* const buffer, int bufferSize);
int tsCompressBigint(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                     char* const buffer, int bufferSize);
int tsCompressBool(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorithm,
                   char* const buffer, int bufferSize);
int tsCompressString(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                     char* const buffer, int bufferSize);
int tsCompressFloat(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                    char* const buffer, int bufferSize);
int tsCompressDouble(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                     char* const buffer, int bufferSize);
int tsCompressTimestamp(const char* const input, int inputSize, const int nelements, char* const output, int outputSize, char algorith,
                        char* const buffer, int bufferSize);

int tsDecompressTinyint(const char* const input, int compressedSize, const int nelements, char* const output,
                        int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressSmallint(const char* const input, int compressedSize, const int nelements, char* const output,
                         int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressInt(const char* const input, int compressedSize, const int nelements, char* const output, int outputSize,
                    char algorithm, char* const buffer, int bufferSize);
int tsDecompressBigint(const char* const input, int compressedSize, const int nelements, char* const output,
                       int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressBool(const char* const input, int compressedSize, const int nelements, char* const output,
                     int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressString(const char* const input, int compressedSize, const int nelements, char* const output,
                       int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressFloat(const char* const input, int compressedSize, const int nelements, char* const output,
                      int outputSize, char algorithm, char* const buffer, int bufferSize);
int tsDecompressDouble(const char* const input, int compressedSize, const int nelements, char* const output,
                       int outputSize, char algorith, char* const buffer, int bufferSize);
int tsDecompressTimestamp(const char* const input, int compressedSize, const int nelements, char* const output,
                          int outputSize, char algorithm, char* const buffer, int bufferSize);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCOMPRESSION_H
