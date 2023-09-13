/*
 * Copyright (c) 2019 Luomi, Inc.
 *
 */
#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <errno.h>

#include "db.h"
#include "tlog.h"

#include <os.h>
#include <tchecksum.h>

int saveToFile(const void* handle);
int readFromFile(const void* handle,int64_t fileIndex);
int compressNone(const void* handle,INDEX* const pIndex);
int compressTaosOneStep(const void* handle,INDEX* const pIndex);
int compressTaosTwoStep(const void* handle,INDEX* const pIndex);
int compressZlib(const void* handle,INDEX* const pIndex);
int compressZstd(const void* handle,INDEX* const pIndex);

int decompressTaosOneStep(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);
int decompressNone(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);
int decompressZlib(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);
int decompressTaosTwoStep(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);
int decompressZstd(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);

int64_t gMillisecondOfHour = 60 * 60 * 1000;
int gErrorCode = 0;
char *gStrError[] = {"success",
                   "database not exist",
				   "out of memory",
				   "memory corrupted",
				   "database already exist",
				   "string too long",//5
};

int gColTypeSize[] = {sizeof(char),sizeof(char),sizeof(short),sizeof(int),sizeof(int64_t),sizeof(float),sizeof(double),MAX_COL_STRING_LEN,sizeof(int64_t),0};
char* gColTypeFormat[] = {"d","%d","%d","%d","%lld","%f","%f","%s","%lld",""};
int (*gCompress[])(const void* handle,INDEX* const pIndex) = {compressNone, compressZlib, compressTaosOneStep,compressTaosTwoStep, compressZstd};
int (*gDecompress[])(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize) = {decompressNone, decompressZlib, decompressTaosOneStep,decompressTaosTwoStep, decompressZstd};

int create_db(char* root,char* name,unsigned char colCount,COL_TYPE colType[],unsigned short cacheSize,STORE_TYPE storeType,COMPRESS_TYPE compressType,unsigned char hoursPerFile){

	gErrorCode = 0;

	if((strlen(root) + strlen(name)) > 100){
		return -1;
	}

	if((colCount+1) > MAX_COL_COUNT){
		return -2;
	}

	struct stat dirstat;
	char dbLogName[128];
	sprintf(dbLogName,"%s/%s/log.data",root,name);
	if (stat(dbLogName, &dirstat) == 0){
		return -3;
	}

	char dbPath[128];
	sprintf(dbPath,"%s/%s",root,name);
	if (stat(dbPath, &dirstat) < 0){
		mkdir(dbPath, 0755);
	}

	int logFd = open(dbLogName, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	if (logFd < 0) {
		return -4;
	}

	unsigned char maxColSize = sizeof(int64_t);
	unsigned short rowSize = sizeof(int64_t);//timestamp
	for(int i = 0; i< colCount; i++){
		int colSize = gColTypeSize[colType[i]];
		rowSize += colSize;
		maxColSize = maxColSize < colSize ? colSize : maxColSize;
	}

	unsigned short dataPos = sizeof(LOG_HEAD) + colCount * sizeof(COL_TYPE);

	int mappingSize = sizeof(LOG_HEAD) + rowSize * cacheSize;
	if (posix_fallocate(logFd, 0, mappingSize) != 0) {
		close(logFd);
		unlink(dbLogName);
		return -5;
	}

	stat(dbLogName, &dirstat);
	int64_t length = dirstat.st_size;
	if (length != mappingSize) {
		close(logFd);
		unlink(dbLogName);
	    return -6;
	}

	LOG_HEAD head;
	memset(&head,0,sizeof(head));
	head.storeType = (compressType == COMPRESS_TAOS_ONE_STEP || compressType == COMPRESS_TAOS_TWO_STEP)?STORE_BY_COL:storeType;
	head.compressType = compressType;
	head.hoursPerFile = hoursPerFile;
	head.cacheSize = cacheSize;
	head.colCount = colCount+1;
	head.dataPos = dataPos;
	head.rowSize = rowSize;
	head.maxColSize = maxColSize;
	head.colType[0] = COL_TYPE_TIMESTAMP;

	write(logFd,&head,sizeof(head));
	write(logFd,colType,colCount*sizeof(COL_TYPE));

	close(logFd);
	return 0;
}

void* open_db(char* name){
	gErrorCode = 0;

	taosResolveCRC();

	struct stat st;
	char dbLogName[128];
	sprintf(dbLogName,"%s/log.data",name);
	if (stat(dbLogName, &st) < 0){
		gErrorCode = 1;
		return NULL;
	}

	int logFd = open(dbLogName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
	if (logFd < 0) {
		gErrorCode = 2;
		return NULL;
	}

#ifndef LINUX
	HANDLE mapHandle = CreateFileMapping(logFd, NULL, PAGE_READWRITE,(DWORD) ((uint64_t) st.st_size >> 32),(DWORD) (st.st_size & 0xffffffff),NULL);
	if (mapHandle == NULL) {
		close(logFd);
		gErrorCode = 3;
		return NULL;
	}
	void* pMem = MapViewOfFile(mapHandle, FILE_MAP_ALL_ACCESS,0,0,st.st_size);
	if (pMem == NULL) {
		CloseHandle(mapHandle);
		close(logFd);
		gErrorCode = 3;
		return NULL;
	}
#else
	void* pMem = mmap(0, st.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, logFd, 0);
	if (pMem == MAP_FAILED) {
		close(logFd);
		gErrorCode = 3;
		return NULL;
	}

#endif

	DB* pDb = malloc(sizeof(DB));
	if(pDb == NULL){
		gErrorCode = 4;
		return pDb;
	}

	memset(pDb, 0, sizeof(DB));
	memcpy(pDb->name,name,strlen(name));
	pDb->signature = pDb;
	pDb->logFd = logFd;
	pDb->pLogHead = (LOG_HEAD*)pMem;
	pDb->pLogData = (char*)pMem + pDb->pLogHead->dataPos;
#ifndef LINUX
	pDb->mapHandle = mapHandle;
#endif

	pthread_mutex_init(&(pDb->dmutex), NULL);

	return pDb;
}

void close_db(void* handle){
	gErrorCode = 0;
	DB* pDb = (DB*)handle;

	if(pDb && pDb->signature == handle){
		int mappingSize = sizeof(LOG_HEAD) + pDb->pLogHead->rowSize * pDb->pLogHead->cacheSize;

#ifndef LINUX
		UnmapViewOfFile(pDb->pLogHead);
		CloseHandle(pDb->mapHandle);
#else
		if(pDb->pLogHead) munmap(pDb->pLogHead, mappingSize);
#endif
		if(pDb->logFd) close(pDb->logFd);
		if(pDb->indexFd) close(pDb->indexFd);
		if(pDb->recordFd) close(pDb->recordFd);

		pthread_mutex_destroy(&(pDb->dmutex));
		free(pDb);
	}

}

int64_t getTimestamp() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
}

int put_db(void* handle,char* pData){
	gErrorCode = 0;
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		logError("put_db","signature  invalidate");
		gErrorCode = 3;
		return -1;
	}

	int64_t st = getTimestamp();

	pthread_mutex_lock(&pDb->dmutex);

	if(pDb->pLogHead->curRowCount > 0 ){
		if(pDb->pLogHead->curRowCount >= pDb->pLogHead->cacheSize){
			saveToFile(handle);
		}else{
			int64_t expectFileIindex = st / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
			int64_t curFileIndex = pDb->pLogHead->startTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
			if(curFileIndex != expectFileIindex){
				saveToFile(handle);
			}
		}
	}

	if(pDb->pLogHead->storeType == STORE_BY_ROW){
		char* pos = pDb->pLogData + pDb->pLogHead->rowSize * pDb->pLogHead->curRowCount;
		memcpy(pos,&st,sizeof(st));
		memcpy(pos+sizeof(st),pData,pDb->pLogHead->rowSize-sizeof(st));
	}else{
		char* pos = pDb->pLogData;
		memcpy(pos + sizeof(st) * pDb->pLogHead->curRowCount,&st,sizeof(st));

		for(int i = 1; i < pDb->pLogHead->colCount; i++){
			int size = gColTypeSize[pDb->pLogHead->colType[i]];
			pos += gColTypeSize[pDb->pLogHead->colType[i-1]] * pDb->pLogHead->cacheSize;
			memcpy(pos + size * pDb->pLogHead->curRowCount,pData,size);

			pData += size;
		}
	}
	if(pDb->pLogHead->curRowCount == 0) pDb->pLogHead->startTimestamp = st;
	pDb->pLogHead->endTimestamp = st;
	pDb->pLogHead->curRowCount++;

	if(pDb->pLogHead->curRowCount >= pDb->pLogHead->cacheSize){
		saveToFile(handle);
	}

	pthread_mutex_unlock(&pDb->dmutex);

	return 0;
}

int db_errcode(){
	return gErrorCode;
}

char *db_errstr(){
	//return gStrError[gErrorCode];
	return NULL;
}

int readFromFile(const void* handle,int64_t fileIndex){
	gErrorCode = 0;
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		logError("readFromFile","signature  invalidate");
		gErrorCode = 3;
		return -1;
	}

	char path[256];
	sprintf(path,"%s/index%lld.data",pDb->name,fileIndex);
	int indexFd = open(path, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
	if(indexFd < 3){
		logError("readFromFile","indexFd < 3");
		gErrorCode = 3;
		return -1;
	}

	sprintf(path,"%s/record%lld.data",pDb->name,fileIndex);
	int recordFd = open(path, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
	if(recordFd < 3){
		logError("readFromFile","recordFd < 3");
		gErrorCode = 3;
		return -1;
	}


	char* pBuf = NULL;
	int len = 0;
	int indexSize = sizeof(INDEX) + sizeof(int)*pDb->pLogHead->colCount;
	INDEX* pIndex = malloc(indexSize);
	lseek(indexFd, 0, SEEK_SET);
	while(read(indexFd,pIndex,indexSize) == indexSize){
		TSCKSUM chksum = 0;

		lseek(recordFd, 0, pIndex->dataOffset);
		for(int i = 0; i < pDb->pLogHead->colCount; i++){
			if(len < pIndex->colLens[i]){
				len = pIndex->colLens[i];
				pBuf = realloc(pBuf,len);
			}

			if(read(recordFd,pBuf,pIndex->colLens[i]) != pIndex->colLens[i]){
				logError("readFromFile","read(pDb->recordFd,pBuf,pIndex->colLens[i]) != pIndex->colLens[i],reason:%s",strerror(errno));
				return -2;
			}

			memcpy(&chksum,pBuf + pIndex->colLens[i] - sizeof(TSCKSUM),sizeof(TSCKSUM));
			if (chksum != taosCalcChecksum(0, (uint8_t *)pBuf, pIndex->colLens[i] - sizeof(TSCKSUM))) {
				logError("readFromFile","data column checksum error, col: %d", i);
				return -3;
			}

			//gDecompress[pDb->pLogHead->compressType](handle,pBuf,pIndex,i);
		}
	}

	free(pBuf);
	free(pIndex);

	return 0;
}

int saveToFile(const void* handle){
	gErrorCode = 0;
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		logError("saveToFile","signature  invalidate");
		gErrorCode = 3;
		return -1;
	}

	int64_t curFileIndex = *((int64_t*)pDb->pLogData) / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
	if(pDb->curFileIndex != curFileIndex){
		if(pDb->indexFd) close(pDb->indexFd);
		if(pDb->recordFd) close(pDb->recordFd);
		pDb->curFileIndex = curFileIndex;
		char path[256];
		sprintf(path,"%s/index%lld.data",pDb->name,curFileIndex);
		pDb->indexFd = open(path, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

		sprintf(path,"%s/record%lld.data",pDb->name,curFileIndex);
		pDb->recordFd = open(path, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	int indexSize = sizeof(INDEX) + sizeof(int)*pDb->pLogHead->colCount;
	INDEX* pIndex = calloc(indexSize,1);
	pIndex->dataOffset = lseek(pDb->recordFd, 0, SEEK_END);

	int len = gCompress[pDb->pLogHead->compressType](handle,pIndex);
	if(len > 0){
		pIndex->rowCount = pDb->pLogHead->curRowCount;
		pIndex->dataLength = len;
		pIndex->startTimestamp = pDb->pLogHead->startTimestamp;
		pIndex->endTimestamp = pDb->pLogHead->endTimestamp;

		lseek(pDb->indexFd, 0, SEEK_END);
		write(pDb->indexFd, pIndex, indexSize);

		//memset(pDb->pLogData,0,pDb->pLogHead->rowSize * pDb->pLogHead->cacheSize);
		pDb->pLogHead->endTimestamp = 0;
		pDb->pLogHead->startTimestamp = 0;
		pDb->pLogHead->curRowCount = 0;
	}
	free(pIndex);


	return 0;
}

void logError(const char *const flags, const char *const format, ...) {
  va_list        argpointer;
  char           buffer[MAX_LOGLINE_SIZE + 10] = {0};
  int            len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                  ptm->tm_min, ptm->tm_sec, (int)timeSecs.tv_usec);
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  len += vsnprintf(buffer + len, 900, format, argpointer);
  va_end(argpointer);

  if (len > MAX_LOGLINE_SIZE) len = MAX_LOGLINE_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  int fd = open("error.txt", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if(fd){
	  write(fd, buffer, len);
	  close(fd);
  }
  write(2, buffer, len);
}
