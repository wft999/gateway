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
#include <ttime.h>

extern int64_t gMillisecondOfHour;

extern int (*gDecompress[])(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize);
int searchFileIndex(void* handle);

BLOCK_TYPE get_block_type(int64_t dataStart, int64_t dataEnd, int64_t targetStart, int64_t targetEnd){
	BLOCK_TYPE ret = BLOCK_NONE;

	if(dataStart > dataEnd || targetStart > targetEnd)
		return ret;
	if(dataStart == targetStart || dataEnd == targetEnd)
		return ret;

	if(dataEnd < targetStart)
		ret = BLOCK_LEFT;
	else if(dataStart > targetEnd)
		ret = BLOCK_RIGHT;
	else if(dataEnd <= targetEnd){
		if(dataStart < targetStart)
			ret = BLOCK_LEFT_CROSS;
		else
			ret = BLOCK_CONTAINED;
	}else{
		if(dataStart <= targetStart)
			ret = BLOCK_CONTAINING;
		else
			ret = BLOCK_RIGHT_CROSS;
	}

	return ret;

}

void test(){
	int fd = open("/root/temp/compressNone/test.data", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	int64_t t = getTimestamp();
	int64_t data[1024] = {t};
	//int64_t data2[1024] = {0};
/*	for(int i=0;i<1000;i++){
		data[i] = t;
		t += 1000;
	}*/
	int size = 1000*sizeof(int64_t);
	size += sizeof(TSCKSUM);
	taosCalcChecksumAppend(0, (uint8_t*)data, size);
	write(fd, data, size);
	close(fd);

	fd = open("/root/temp/compressNone/test.data", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	read(fd, data, size);
	close(fd);
	TSCKSUM chksum = 0;
	memcpy(&chksum,((uint8_t *)data) + size - sizeof(TSCKSUM),sizeof(TSCKSUM));
	TSCKSUM chksum2 = taosCalcChecksum(0, (uint8_t *)data, size - sizeof(TSCKSUM));
}

QUERY* open_query(const void* handle,char* strStartTime, char* strEndTime,ORDER_TYPE order){//2018-06-01 08:00:00.000
	int64_t startTime, endTime;


	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		logError("readFromFile","signature  invalidate");
		return NULL;
	}

	if (taosParseTime(strStartTime, &startTime, strlen(strStartTime), TSDB_TIME_PRECISION_MILLI) != 0) {
		logError("open_query","strStartTime invalidate");
		return NULL;
	}

	if(strcmp(strEndTime,"NOW") == 0 || strcmp(strEndTime,"now") == 0){
		endTime = getTimestamp();
	}else{
		if (taosParseTime(strEndTime, &endTime, strlen(strEndTime), TSDB_TIME_PRECISION_MILLI) != 0) {
			logError("open_query","strEndTime invalidate");
		    return NULL;
		}
	}

	int bufSize = pDb->pLogHead->rowSize * pDb->pLogHead->cacheSize + 100;
	QUERY* pQuery = (QUERY*)malloc(sizeof(QUERY) + bufSize);
	if(pQuery == NULL){
		logError("open_query","pQuery == NULL");
		return NULL;
	}
	memset(pQuery,0,sizeof(QUERY));
	pQuery->signature = pQuery;
	pQuery->curDataIndex = -1;
	pQuery->curBlockIndex = -1;
	pQuery->curFileIndex = -1;
	pQuery->pDb = pDb;
	pQuery->order = order;
	pQuery->endTimestamp = endTime > startTime ? endTime : startTime;
	pQuery->startTimestamp = startTime < endTime ? startTime : endTime;
	pQuery->colBitmap = 0XFFFFFFFFFFFFFFFF;

	if(pQuery->order == ORDER_ASC){
		pQuery->endFileIndex = pQuery->endTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
		pQuery->startFileIndex = pQuery->startTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
	}else{
		pQuery->endFileIndex = pQuery->startTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
		pQuery->startFileIndex =  pQuery->endTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);
	}

	return pQuery;
}

int binary_search(int64_t* data,int64_t key,int count,int isStart) {
/*	if(isStart){
		int i = 0;
		while(i < count){
			if(data[i] >= key)
				return i;
			i++;
		}
	}else{
		int i = count -1;
		while(i >= 0){
			if(data[i] <= key)
				return i;
			i++;
		}
		return -1;
	}*/
    int low,center,high;
    low = 0;
    high = count - 1;

    int start,end;
    while(low <= high) {
        center = (low + high) / 2;
        if(data[center] == key) {
            return center;
        } else if(data[center] > key) {
            high = center - 1;
            start = center;
        } else {
            low = center + 1;
            end = center;
        }
    }
    return isStart > 0 ? start : end;
}

int set_data_index(void* handle,BLOCK_TYPE type){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}

	DB* pDb = pQuery->pDb;

	if(type == BLOCK_EQUAL || type == BLOCK_CONTAINED){
		pQuery->startDataIndex = pQuery->order == ORDER_ASC ? 0 : pQuery->rowCount-1;
		pQuery->endDataIndex = pQuery->order == ORDER_ASC ? pQuery->rowCount-1 : 0;
	}else if(type == BLOCK_LEFT_CROSS){
		int start = binary_search((int64_t*)pQuery->buf,pQuery->startTimestamp,pQuery->rowCount,1);
		pQuery->startDataIndex = pQuery->order == ORDER_ASC ? start : pQuery->rowCount-1;
		pQuery->endDataIndex = pQuery->order == ORDER_ASC ? pQuery->rowCount-1 : start;
	}else if(type == BLOCK_RIGHT_CROSS){
		int end = binary_search((int64_t*)pQuery->buf,pQuery->endTimestamp,pQuery->rowCount,0);
		pQuery->startDataIndex = pQuery->order == ORDER_ASC ? 0 : end;
		pQuery->endDataIndex = pQuery->order == ORDER_ASC ? end : 0;
	}else if(type == BLOCK_CONTAINING){
		int start = binary_search((int64_t*)pQuery->buf,pQuery->startTimestamp,pQuery->rowCount,1);
		int end = binary_search((int64_t*)pQuery->buf,pQuery->endTimestamp,pQuery->rowCount,0);
		pQuery->startDataIndex = pQuery->order == ORDER_ASC ? start : end;
		pQuery->endDataIndex = pQuery->order == ORDER_ASC ? end : start;
	}
	pQuery->curDataIndex = pQuery->startDataIndex;

	return 0;
}

int check_block(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery || pQuery->indexFd < 3 || pQuery->recordFd < 3){
		return -1;
	}

	DB* pDb = pQuery->pDb;
	int indexSize = sizeof(INDEX) + sizeof(int)*pDb->pLogHead->colCount;
	INDEX* pIndex = calloc(indexSize,1);

	lseek(pQuery->indexFd, pQuery->curBlockIndex * indexSize,SEEK_SET);
	read(pQuery->indexFd, pIndex, indexSize);

	BLOCK_TYPE type = get_block_type(pIndex->startTimestamp,pIndex->endTimestamp,pQuery->startTimestamp,pQuery->endTimestamp);
	if(type < BLOCK_EQUAL){
		free(pIndex);
		return -1;
	}

	int len = 0;
	TSCKSUM chksum = 0;
	char* pInput = NULL;
	char* pOutput = pQuery->buf;

	lseek(pQuery->recordFd, pIndex->dataOffset,SEEK_SET);
	for(int i = 0; i < pDb->pLogHead->colCount; i++){
		if(len < pIndex->colLens[i]){
			len = pIndex->colLens[i];
			pInput = realloc(pInput,len+100);
		}

		COL_TYPE type = pDb->pLogHead->colType[i];
		int colSize = gColTypeSize[type];
		int outSize = colSize * pDb->pLogHead->cacheSize;

		if(pQuery->colBitmap & (1 << i)){
			if(read(pQuery->recordFd,pInput,pIndex->colLens[i]) != pIndex->colLens[i]){
				logError("check_block","read(pDb->recordFd,pBuf,pIndex->colLens[i]) != pIndex->colLens[i],reason:%s",strerror(errno));
				return -2;
			}

/*			memcpy(&chksum,pInput + pIndex->colLens[i] - sizeof(TSCKSUM),sizeof(TSCKSUM));
			TSCKSUM chksum2 = taosCalcChecksum(0, (uint8_t *)pInput, pIndex->colLens[i] - sizeof(TSCKSUM));
			if(chksum != chksum2)*/
			if(taosCheckChecksumWhole(pInput,pIndex->colLens[i]) == 0)
			{
				logError("check_block","data column checksum error, col: %d", i);
				return -3;
			}

			gDecompress[pDb->pLogHead->compressType](pDb,pInput,pIndex,i,pOutput,outSize);
		}else{
			lseek(pQuery->recordFd, pIndex->colLens[i], SEEK_CUR);
		}

		pOutput += outSize;
	}

	int64_t* t = (int64_t*)pQuery->buf;

	pQuery->rowCount = pIndex->rowCount;
	set_data_index(handle,type);
	free(pIndex);

	return 0;
}

int check_log(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}
	pQuery->logChecked = 1;

	DB* pDb = pQuery->pDb;
	BLOCK_TYPE type = get_block_type(pDb->pLogHead->startTimestamp,pDb->pLogHead->endTimestamp,pQuery->startTimestamp,pQuery->endTimestamp);
	if(type < BLOCK_EQUAL){
		return -1;
	}

	memcpy(pQuery->buf,pDb->pLogData,pDb->pLogHead->rowSize * pDb->pLogHead->cacheSize);
	pQuery->rowCount = pDb->pLogHead->curRowCount;
	set_data_index(handle,type);

	return 0;
}

int searchBlockIndex(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}

	DB* pDb = pQuery->pDb;

	while(1){
		if(pQuery->order == ORDER_ASC){
			if(pQuery->curBlockIndex > pQuery->endBlockIndex)
				break;
		}else{
			if(pQuery->curBlockIndex < pQuery->endBlockIndex)
				break;
		}

		if(check_block(handle) == 0)
			return 0;

		pQuery->curBlockIndex += pQuery->order;
	}

	pQuery->curBlockIndex = -1;
	pQuery->startBlockIndex = -1;
	pQuery->endBlockIndex = -1;

	return -1;
}

int checkFile(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}
	DB* pDb = pQuery->pDb;
	pDb->curFileIndex = pDb->pLogHead->startTimestamp / (pDb->pLogHead->hoursPerFile * gMillisecondOfHour);

	pQuery->startBlockIndex = -1;
	pQuery->endBlockIndex = -1;
	pQuery->curBlockIndex = -1;

	char path[256];
	struct stat statbuf;
	sprintf(path,"%s/index%lld.data",pDb->name,pQuery->curFileIndex);

	if(stat(path, &statbuf) < 0){
		if(pQuery->curFileIndex != pDb->curFileIndex){
			return -1;
		}
	}else{
		if(pQuery->order == ORDER_ASC){
			pQuery->startBlockIndex = 0;
			pQuery->endBlockIndex = statbuf.st_size / sizeof(INDEX) - 1;
		}else{
			pQuery->startBlockIndex = statbuf.st_size / sizeof(INDEX) - 1;
			pQuery->endBlockIndex = 0;
		}
		pQuery->curBlockIndex = pQuery->startBlockIndex;

		if(pQuery->indexFd) close(pQuery->indexFd);
		int fd = open(path, O_RDONLY,0);
		if(fd < 0) return -1;
		pQuery->indexFd = fd;

		sprintf(path,"%s/record%lld.data",pDb->name,pQuery->curFileIndex);
		if(pQuery->recordFd) close(pQuery->recordFd);
		fd = open(path, O_RDONLY,0);
		if(fd < 0){
			close(pQuery->indexFd);
			return -1;
		}
		pQuery->recordFd = fd;
	}

	if(searchBlockIndex(handle) == 0)
		return 0;

	return -1;
}

int searchFileIndex(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}

	DB* pDb = pQuery->pDb;
	while(1){
		if(pQuery->order == ORDER_ASC){
			if(pQuery->curFileIndex > pQuery->endFileIndex)
				return -1;
		}else{
			if(pQuery->curFileIndex < pQuery->endFileIndex)
				return -1;
		}

		if(checkFile(handle) == 0){
			return 0;
		}

		pQuery->curFileIndex += pQuery->order;
	}

	return -1;
}

int move_next(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return -1;
	}

	if(pQuery->order == ORDER_DESC && pQuery->logChecked == 0){
		if(check_log(handle) == 0)
			return 0;
	}

	if(pQuery->curDataIndex == -1 && pQuery->curFileIndex == -1){
		pQuery->curFileIndex = pQuery->startFileIndex;
		if(searchFileIndex(handle) == 0)
			return 0;
	}

	if(pQuery->curDataIndex != -1  && pQuery->curDataIndex == pQuery->endDataIndex){
		pQuery->curDataIndex = -1;
		pQuery->startDataIndex = -1;
		pQuery->endDataIndex = -1;

		if(pQuery->curFileIndex == -1){
			pQuery->curFileIndex = pQuery->startFileIndex;
			if(searchFileIndex(handle) == 0)
				return 0;
		}else{
			pQuery->curBlockIndex += pQuery->order;
			if(searchBlockIndex(handle) == 0)
				return 0;

			pQuery->curFileIndex += pQuery->order;
			if(searchFileIndex(handle) == 0)
				return 0;
		}
	}

	if(pQuery->curDataIndex != -1){
		pQuery->curDataIndex += pQuery->order;
		return 0;
	}

	if(pQuery->order == ORDER_ASC && pQuery->logChecked == 0){
		if(check_log(handle) == 0)
			return 0;
	}

	return -1;
}

void* fetch_col(void* handle, int colIndex){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return NULL;
	}
	DB* pDb = pQuery->pDb;

	if(colIndex >= pDb->pLogHead->colCount)
		return NULL;

	int size;
	char* pos = pQuery->buf;
	for(int i = 0; i < colIndex; i++){
		size = gColTypeSize[pDb->pLogHead->colType[i]];
		pos += size * pDb->pLogHead->cacheSize;
	}

	size = gColTypeSize[pDb->pLogHead->colType[colIndex]];
	pos += size * pQuery->curDataIndex;

	return pos;
}

void close_query(void* handle){
	QUERY* pQuery = (QUERY*)handle;
	if(pQuery == NULL || pQuery->signature != pQuery){
		return;
	}

	if(pQuery->indexFd) close(pQuery->indexFd);
	if(pQuery->recordFd) close(pQuery->recordFd);

	free(handle);
}
