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

typedef enum {
	FIELD_TYPE_BOOL,
	FIELD_TYPE_INTEGER,
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
#define BITS_PER_BYTE 8
#define FILE_BLOCK_NUM				4

#define CACHE_ROW_NUM			1024
#define CACHE_BITMAP_SIZE		CACHE_ROW_NUM/BITS_PER_BYTE
#define DEFAULT_FIELD_NUM		20000
#define MAX_QUERY_FILE			10
#define MIN_QUERY_BUFFER		32
#define RETENTION_HOURS_MAX 	720 //30days
#define RETENTION_MBS_MAX 		20480 //20GB

typedef struct _CACHE{
	unsigned char isNone[CACHE_BITMAP_SIZE];
	union{
		unsigned char bvalue[CACHE_BITMAP_SIZE];
		float fvalue[CACHE_ROW_NUM];
		double dvalue[CACHE_ROW_NUM];
	};
}CACHE,*PCACHE;

typedef struct _FIELD{
	char* name;
	unsigned int id;
	FIELD_TYPE type;
	int64_t typeChangeTimestamp;
	CACHE cache[2];
}FIELD,*PFIELD;

typedef struct _DB{
	void* signature;
	char name[128];

	COMPRESS_TYPE gCompressType;
	unsigned int retentionHours;
	unsigned int retentionMbs;

	unsigned long cacheId;
	int64_t preStartTimestamp;
	int64_t startTimestamp;
	int curRowNum;

	int logFd[2];
	int indexFd,dataFd;
	int curFileBlockNum;

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


typedef struct _QUERY{
	void* signature;

	int64_t startTimestamp;
	int64_t endTimestamp;
	ORDER_TYPE order;


	int		logChecked;

	int64_t startFileIndex;
	int64_t endFileIndex;
	int64_t curFileIndex;

	BLOCK_TYPE		blockType;
	int32_t		startBlockIndex;
	int32_t		endBlockIndex;
	int32_t		curBlockIndex;

	int32_t		rowCount;
	int32_t		startDataIndex;
	int32_t		endDataIndex;
	int32_t		curDataIndex;

	int 	recordFd;
	int 	indexFd;
	INDEX	indexRecord;

	char 	reserved[16];
	char 	buf[0];
}QUERY;

int open_db(char* name,unsigned int retentionHours,unsigned int retentionMbs);
void close_db();
int put_db(char* pData);
int put_db_json(char* pData);
int64_t getTimestamp();
int dump(char* strFileName,char* device,char* field);
int query(char* device,char* field,char* strStartTime, char* strEndTime,ORDER_TYPE order,char* buffer,int buffer_size);


#ifdef __cplusplus
}
#endif

#endif /* EDB_DB_H_ */
