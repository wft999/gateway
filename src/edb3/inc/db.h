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

#include "dict.h"
#include "gorilla.h"

typedef enum {
	FIELD_TYPE_BOOL,
	FIELD_TYPE_DOUBLE,
} FIELD_TYPE;


typedef enum {
	COMPRESS_NONE,
	COMPRESS_ZLIB,
	COMPRESS_TAOS_ONE_STEP,
	COMPRESS_TAOS_TWO_STEP,
	COMPRESS_ZSTD,
}COMPRESS_TYPE;
typedef enum {
	BLOCK_NONE,
	BLOCK_LEFT,
	BLOCK_RIGHT,
	BLOCK_EQUAL,
	BLOCK_LEFT_CROSS,
	BLOCK_CONTAINED,
	BLOCK_CONTAINING,
	BLOCK_RIGHT_CROSS

}BLOCK_TYPE;

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
#define MAX_QUERY_FILE			168
#define MIN_QUERY_BUFFER		32
#define RETENTION_HOURS_MAX 	720 //30days
#define RETENTION_MBS_MAX 		20480 //20GB

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
	unsigned int id;
	FIELD_TYPE type;
	int64_t typeChangeTimestamp;
	int64_t curTimestamp;
	CACHE* cache[2];
}FIELD,*PFIELD;

typedef struct _DB{
	void* signature;
	char name[128];
	int dbReady;
	int logReady;

	COMPRESS_TYPE gCompressType;
	unsigned int retentionHours;
	unsigned int retentionMbs;

	unsigned long cacheId;
	int64_t preStartTimestamp;
	int64_t fileStartTimestamp;
	int64_t startTimestamp;
	int64_t logStartTimestamp;
	int curRowNum;
	int curLogRowNum;

	int logFd[2];
	int indexFd,dataFd;

	pthread_mutex_t dmutex;

	dict *fields;
	unsigned int nextFieldId;
	unsigned int maxFieldNum;
	PFIELD* pFieldsArray;
	unsigned char fieldsChange;

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

typedef struct _FileBlock{
	BLOCK b;
	INDEX i;
}FileBlock,*PFileBlock;

typedef struct _QueryContext{
	int64_t startQueryTime;
	int64_t endQueryTime;
	int count;
	ORDER_TYPE order;

	int64_t selectedTimeStamp[MAX_QUERY_FILE];
	int curFileNum;

	struct _ResultSet* resultSet;

}QueryContext;

typedef struct _ResultSet{
	struct _ResultSet* nextResultSet;
	QueryContext* ctx;
	FIELD *pField;

	int curFileID;

	FileBlock fb[FILE_BLOCK_NUM];
	int fileBlockNum;
	int curFileBlockID;

	CACHE* pCache;
	int cacheRowNum;
	int curCacheRowId;
	int64_t startCacheTime;
	int64_t endCacheTime;

	double* curSample;
	int curSampleSize;
	int curSampleRowId;
}ResultSet;



int open_db(char* name,unsigned int retentionHours,unsigned int retentionMbs);
void close_db();
int put_db(char* pData);
int put_db_json(char* pData);

int64_t getTimestamp();
int dump(char* strFileName,char* device,char* field);
int query(char* device,char* field,char* strStartTime, char* strEndTime,ORDER_TYPE order,char* buffer,int buffer_size);
QueryContext* open_query(char* device,char*fields,int64_t startQueryTime, int64_t endQueryTime,ORDER_TYPE order);
int next_result(QueryContext* ctx,int resultSetId,int64_t* timeStamp,char* value);
void close_query(QueryContext* ctx);

#ifdef __cplusplus
}
#endif

#endif /* EDB_DB_H_ */
