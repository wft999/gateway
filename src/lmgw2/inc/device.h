/*
 * device.h
 *
 *  Created on: 2023Äê9ÔÂ4ÈÕ
 *      Author: a
 */

#ifndef SRC_LMGW2_INC_DEVICE_H_
#define SRC_LMGW2_INC_DEVICE_H_
#include "dict.h"
#include "module.h"
#include "rule.h"
#include "indexer.h"

#define BITS_PER_BYTE 			8
#define FIELD_FLAG_NONE 0
#define FIELD_FLAG_ACC (1<<0)
#define CACHE_ROW_NUM			1024
#define CACHE_BITMAP_SIZE		CACHE_ROW_NUM/BITS_PER_BYTE

typedef enum {
	FLAG_OP_INVALID = -1,
	FLAG_OP_RESET = 0,
	FLAG_OP_SET = 1
}FLAG_OP_TYPE;
typedef enum {
	FIELD_TYPE_BOOL,
	FIELD_TYPE_TING_INT,
	FIELD_TYPE_SMALL_INT,
	FIELD_TYPE_INT,
	FIELD_TYPE_BIG_INT,
	FIELD_TYPE_FLOAT,
	FIELD_TYPE_DOUBLE,
	FIELD_TYPE_TIMESTAMP,
	FIELD_TYPE_MAX_TYPES
} FIELD_TYPE;

typedef struct _CACHE{
	unsigned char haveTimestamp[CACHE_BITMAP_SIZE];
	unsigned char isValueNone[CACHE_BITMAP_SIZE];
	union{
		unsigned char bvalue[CACHE_BITMAP_SIZE];
		float fvalue[CACHE_ROW_NUM];
		double dvalue[CACHE_ROW_NUM];
	};
}CACHE,*PCACHE;

typedef struct _FIELD{
	FIELD_TYPE type;
	uint64_t flags;
	long long typeChangeTimestamp;
	RedisModuleString *keyName;
	RedisModuleString *func;
	RedisModuleString **args;
	size_t	args_len;
	CompactionRule *rules;

	CACHE* cache;
}FIELD,*PFIELD;

typedef struct Device
{
	dict *fields;
    RedisModuleString *keyName;
    size_t totalSamples;
    RedisModuleDict *pDeviceIndex;
    timestamp_t timestamps[CACHE_ROW_NUM];

    Label *labels;
    size_t labelsCount;
} Device;

Device* NewDevice(RedisModuleString *keyName);
void FreeDevice(void *value);
void FreeIndex(RedisModuleDict *index);
void ResetDevice(Device* pDevice);
int GetDevice(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
			  Device **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent);

FIELD* NewField(RedisModuleString *keyName,FIELD_TYPE type,uint64_t flags);
void ReplyFieldInfo(RedisModuleCtx *ctx,FIELD* pField);

int bitmapTestBit(unsigned char *bitmap, int pos);
void bitmapSetBit(unsigned char *bitmap, int pos);
void bitmapClearBit(unsigned char *bitmap, int pos);


int getFieldTypeSize(FIELD* pField);
uint64_t StringLenFlagToConst(const char *flag, size_t len);
int StringLenOpTypeToEnum(const char *op_type, size_t len);

#endif /* SRC_LMGW2_INC_DEVICE_H_ */
