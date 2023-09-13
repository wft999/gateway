/*
 * db.h
 *
 *  Created on: 2019��9��30��
 *      Author: a
 */

#ifndef EDB_DB_H_
#define EDB_DB_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../../lmgw/inc/redismodule.h"
#include "../../lmgw/inc/module.h"
#include "../../lmgw/inc/dict.h"
#include "../../lmgw/inc/gorilla.h"
#include "../../lmgw/inc/compaction.h"

typedef enum {
	FIELD_TYPE_BOOL,
	FIELD_TYPE_DOUBLE,
} FIELD_TYPE;

typedef enum {
	ORDER_ASC = 1,
	ORDER_DESC = -1
}ORDER_TYPE;

#define THREAD_STACK_SIZE (1024*1024*4)
#define UNUSED(V) ((void) V)
#define BITS_PER_BYTE 			8
#define FILE_BLOCK_NUM			12

#define CACHE_ROW_NUM			304
#define CACHE_BITMAP_SIZE		CACHE_ROW_NUM/BITS_PER_BYTE
#define CHUNK_DEFAULT_SIZE		CACHE_ROW_NUM * 4
#define CHUNK_RESIZE_STEP 		32

#define DEFAULT_FIELD_NUM		20000
#define MAX_QUERY_FILE			10
#define MIN_QUERY_BUFFER		32
#define RETENTION_HOURS_MAX 	720 //30days
#define RETENTION_MBS_MAX 		20480 //20GB

typedef struct CompactionRule
{
    RedisModuleString *destKey;
    timestamp_t bucketDuration;
    timestamp_t timestampAlignment;
    AggregationClass *aggClass;
    TS_AGG_TYPES_T aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
    timestamp_t startCurrentTimeBucket; // Beware that the first bucket is alway starting in 0 no
                                        // matter the alignment
} CompactionRule;

typedef struct _CACHE{
	unsigned char haveTimestamp[CACHE_BITMAP_SIZE];
	unsigned char isValueNone[CACHE_BITMAP_SIZE];
	union{
		unsigned char bvalue[CACHE_BITMAP_SIZE];
		CompressedChunk chunk;
	};
}CACHE,*PCACHE;

typedef struct _FIELD{
	char* name;
	FIELD_TYPE type;
	long long typeChangeTimestamp;
	RedisModuleString *func;
	RedisModuleString *args;
	CompactionRule *rules;
	CACHE* cache[2];
}FIELD,*PFIELD;

typedef struct _DB{
	void* signature;
	char path[128];
	int dbReady;

	//COMPRESS_TYPE gCompressType;
	unsigned int retentionHours;
	unsigned int retentionMbs;

	unsigned long cacheId;
	int64_t preStartTimestamp;
	int64_t fileStartTimestamp;
	int64_t startTimestamp;

	int curRowNum;

	int indexFd,dataFd;

	pthread_mutex_t dmutex;

	dict *fields;

}DB,*PDB;


typedef struct _INDEX{
	int32_t dataOffset;
	int32_t dataLength;
}INDEX;

typedef struct _BLOCK{
	int64_t startTimestamp;
	int32_t	fieldCount;
	int32_t	rowCount;
}BLOCK,*PBLOCK;

int db_ready();
int open_db(RedisModuleCtx *ctx);
int init_db(RedisModuleCtx *ctx, const char* path,unsigned int retentionHours,unsigned int retentionMbs);
void close_db();
int put_db(RedisModuleCtx *ctx, int rowId,FIELD *pField,const char* value);
int put_db_json(char* pData);
void* get_field(RedisModuleCtx *ctx,const char* device, const char* field);
//int64_t getTimestamp();
int dump(char* strFileName,char* device,char* field);
int query(RedisModuleCtx *ctx, const char* device,const char* field,const char* strStartTime, const char* strEndTime,const char*  strOrder);
int get_row_id(int64_t timeStamp);

#ifdef __cplusplus
}
#endif

#endif /* EDB_DB_H_ */
