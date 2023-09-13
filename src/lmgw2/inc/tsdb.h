/*
 * tsdb.h
 *
 *  Created on: 2023Äê7ÔÂ16ÈÕ
 *      Author: a
 */

#ifndef SRC_LMGW2_INC_TSDB_H_
#define SRC_LMGW2_INC_TSDB_H_

#include "endianconv.h"
#include "device.h"

#define EDB_FILE_PERIOD			3600000
#define MAX_NAME_SIZE			32

typedef enum {
	ORDER_ASC = 1,
	ORDER_DESC = -1
}ORDER_TYPE;

typedef enum
{
    QUERY_NORMAL = 0,
	QUERY_SUB = 1,
	QUERY_AGG = 2
} QUERY_TYPE;

typedef struct _QFIELD{
	FIELD* pField;
	struct _QFIELD* next;
	CompactionRule *rules;
	CACHE* curCache;
}QFIELD,*PQFIELD;


typedef struct _DINDEX{
	int32_t dataOffset;
	int32_t dataLength;
	timestamp_t startTimestamp;
	RedisModuleDict *pFieldIndex;
}DINDEX;

typedef struct _DiskDINDEX{
	char name[MAX_NAME_SIZE];
	int32_t dataOffset;
	int32_t dataLength;
	timestamp_t startTimestamp;
	timestamp_t endTimestamp;
	size_t totalFields;
}DiskDINDEX;

typedef struct _FINDEX{
	int32_t dataOffset;
	int32_t dataLength;
}FINDEX;

typedef struct _DiskFINDEX{
	char name[MAX_NAME_SIZE];
	int32_t dataOffset;
	int32_t dataLength;
}DiskFINDEX;

typedef enum
{
    DICT_OP_SET = 0,
    DICT_OP_REPLACE = 1,
    DICT_OP_DEL = 2
} DictOp;


int add_sample(RedisModuleCtx *ctx,int rowId,FIELD *pField,const char* value);

int query_sample(RedisModuleCtx *ctx,Device* pDevice,QFIELD* pFieldHead,int64_t startQueryTime, int64_t endQueryTime,ORDER_TYPE order,QUERY_TYPE query_type);
void LoadCache(RedisModuleCtx *ctx);
void SaveDevice(RedisModuleCtx *ctx,Device* pDevice);
void IndexDeviceFromName(RedisModuleCtx *ctx, RedisModuleString *keyname);

void DeleteOldFile(RedisModuleCtx *ctx);
extern unsigned int retentionHours;
extern unsigned int retentionMbs;
extern RedisModuleString* edb_path;

#endif /* SRC_LMGW2_INC_TSDB_H_ */
