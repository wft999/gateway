/*
 * db.h
 *
 *  Created on: 2019Äê9ÔÂ30ÈÕ
 *      Author: a
 */

#ifndef EDB_DB_H_
#define EDB_DB_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
	COL_TYPE_BOOL,
	COL_TYPE_TING_INT,
	COL_TYPE_SMALL_INT,
	COL_TYPE_INT,
	COL_TYPE_BIG_INT,
	COL_TYPE_FLOAT,
	COL_TYPE_DOUBLE,
	COL_TYPE_STRING,
	COL_TYPE_TIMESTAMP,
	COL_TYPE_MAX_TYPES
} COL_TYPE;

typedef enum {
	STORE_BY_ROW,
	STORE_BY_COL
}STORE_TYPE;

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

#define DB_NAME_LEN 				128
#define MAX_LOGLINE_SIZE           1000
#define MAX_COL_COUNT 				32
#define MAX_COL_STRING_LEN 			32

typedef struct _LOG_HEAD{
	STORE_TYPE storeType;
	COMPRESS_TYPE compressType;
	unsigned char hoursPerFile;
	unsigned char maxColSize;

	unsigned short curRowCount;
	unsigned short cacheSize;
	unsigned short rowSize;
	unsigned short dataPos;

	int64_t startTimestamp;
	int64_t endTimestamp;

	unsigned char colCount;
	char reserved[32];
	COL_TYPE colType[1];
}LOG_HEAD;

typedef struct _DB{
	void* signature;
	char name[128];

	int logFd;
	char* pLogData;
	LOG_HEAD* pLogHead;
#ifndef LINUX
	HANDLE mapHandle;
#endif
	int recordFd;
	int indexFd;
	int64_t curFileIndex;

	pthread_mutex_t dmutex;

}DB,*PDB;

typedef struct _INDEX{
	int32_t rowCount;
	int64_t startTimestamp;
	int64_t endTimestamp;
	int32_t dataOffset;
	int32_t dataLength;

	char reserved[8];
	int32_t colLens[0];
}INDEX;

typedef struct _QUERY{
	void* signature;
	DB* pDb;
	int64_t startTimestamp;
	int64_t endTimestamp;
	ORDER_TYPE order;
	int64_t	colBitmap;

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


int create_db(char* root,char* name,unsigned char colCount,COL_TYPE colType[],unsigned short cacheSize,STORE_TYPE storeType,COMPRESS_TYPE compressType,unsigned char hoursPerFile);
void* open_db(char* name);
void close_db(void* db);
int put_db(void* db,char* pData);
QUERY* open_query(const void* handle,char* strStartTime, char* strEndTime,ORDER_TYPE order);
int move_next(void* handle);
void* fetch_col(void* handle,int colIndex);
void close_query(void* handle);

int db_errcode();
void logError(const char *const flags, const char *const format, ...);


extern int gErrorCode;
extern int gColTypeSize[];


#ifdef __cplusplus
}
#endif

#endif /* EDB_DB_H_ */
