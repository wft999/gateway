/*
 * tsdb.c
 *
 *  Created on: 2023Äê7ÔÂ16ÈÕ
 *      Author: a
 */
#include "tsdb.h"
#include "tscompression.h"
#include "fast_double_parser_c.h"

#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>


int64_t lastDeleteFileId = 0;
extern RedisModuleType *DeviceType;
extern int64_t lastSampleTimestamp;

void encodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

timestamp_t getFileId(timestamp_t timestamp){
	return timestamp - timestamp%(EDB_FILE_PERIOD);
}

int (*gTaosCompFunc[])(const char *const input, int inputSize, const int elements, char *const output, int outputSize,
                   char algorithm, char *const buffer, int bufferSize) = {
                                          tsCompressBool,
                                          tsCompressTinyint,
                                          tsCompressSmallint,
                                          tsCompressInt,
                                          tsCompressBigint,
                                          tsCompressFloat,
                                          tsCompressDouble,
                                          tsCompressString,
                                          tsCompressTimestamp,
                                          tsCompressString};

int (*gTaosDecompFunc[])(const char *const input, int compressedSize, const int elements, char *const output,
                     int outputSize, char algorithm, char *const buffer, int bufferSize) = {
                                                            tsDecompressBool,
                                                            tsDecompressTinyint,
                                                            tsDecompressSmallint,
                                                            tsDecompressInt,
                                                            tsDecompressBigint,
                                                            tsDecompressFloat,
                                                            tsDecompressDouble,
                                                            tsDecompressString,
                                                            tsDecompressTimestamp,
                                                            tsDecompressString};





void FreeIndex(RedisModuleDict *index){
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(index, "^", NULL, 0);
	char *key;
	DINDEX *dindex;
	while ((key = RedisModule_DictNextC(iter, NULL,(void*)&dindex)) != NULL) {
		RedisModuleDictIter *iter2 = RedisModule_DictIteratorStartC(dindex->pFieldIndex, "^", NULL, 0);
		char *key2;
		FINDEX *findex;
		while ((key2 = RedisModule_DictNextC(iter2, NULL,(void*)&findex)) != NULL) {
			RedisModule_Free(findex);
		}
		RedisModule_DictIteratorStop(iter2);
		RedisModule_FreeDict(NULL,dindex->pFieldIndex);

		RedisModule_Free(dindex);
	}
	RedisModule_DictIteratorStop(iter);
	RedisModule_FreeDict(NULL,index);
}

int dictOperator(RedisModuleDict *d, void *index, timestamp_t ts, DictOp op) {
    timestamp_t rax_key = htonu64(ts);
    switch (op) {
        case DICT_OP_SET:
            return RedisModule_DictSetC(d, &rax_key, sizeof(rax_key), index);
        case DICT_OP_REPLACE:
            return RedisModule_DictReplaceC(d, &rax_key, sizeof(rax_key), index);
        case DICT_OP_DEL:
            return RedisModule_DictDelC(d, &rax_key, sizeof(rax_key), NULL);
    }
    index = NULL;
    return REDISMODULE_OK; // silence compiler
}

void trimIndex(RedisModuleCtx *ctx,Device* pDevice){
	timestamp_t rax_key;
	timestamp_t startQueryTime = lastDeleteFileId + EDB_FILE_PERIOD - 1;
	encodeTimestamp(&rax_key, startQueryTime);
	RedisModuleDictIter* dictIter = RedisModule_DictIteratorStartC(pDevice->pDeviceIndex, "<=", &rax_key, sizeof(rax_key));
	RedisModuleString *key;
	DINDEX *dindex;
	while((key = RedisModule_DictPrev(ctx,dictIter,(void**)&dindex)) != NULL) {
		RedisModule_DictDel(pDevice->pDeviceIndex,key,NULL);

		RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dindex->pFieldIndex, "^", NULL, 0);
		RedisModuleString *key2;
		FINDEX *findex;
		while ((key2 = RedisModule_DictNext(ctx,iter,(void**)&findex)) != NULL) {
			RedisModule_Free(findex);
		}
		RedisModule_DictIteratorStop(iter);
		RedisModule_FreeDict(ctx,dindex->pFieldIndex);
		RedisModule_Free(dindex);
	}
	RedisModule_DictIteratorStop(dictIter);
}

void SaveDevice(RedisModuleCtx *ctx,Device* pDevice){
	if(RedisModule_IsAOFClient(RedisModule_GetClientId(ctx)))
		return;

	timestamp_t endTimestamp = pDevice->timestamps[CACHE_ROW_NUM-1];
	timestamp_t file_id = getFileId(endTimestamp);
	char dbPathName[128];
	sprintf(dbPathName,"%s/%" PRIu64 ".data",RedisModule_StringPtrLen(edb_path, NULL),file_id);
	int dataFd = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	sprintf(dbPathName,"%s/%" PRIu64 ".index",RedisModule_StringPtrLen(edb_path, NULL),file_id);
	int indexFd = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

	int output_size = sizeof(double)*CACHE_ROW_NUM;
	int buffer_size = sizeof(double)*CACHE_ROW_NUM;
	char* buffer = RedisModule_Alloc(buffer_size);
	char* output = RedisModule_Alloc(output_size);

	DINDEX* dindex = RedisModule_Alloc(sizeof(DINDEX));
	dindex->pFieldIndex = RedisModule_CreateDict(NULL);
	dindex->startTimestamp = pDevice->timestamps[0];
	dindex->dataOffset = lseek(dataFd, 0, SEEK_END);
	dindex->dataLength = tsCompressTimestamp((const char*)pDevice->timestamps,0,CACHE_ROW_NUM,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
	write(dataFd, output, dindex->dataLength);
	dictOperator(pDevice->pDeviceIndex,dindex,pDevice->timestamps[CACHE_ROW_NUM-1],DICT_OP_SET);

	DiskDINDEX ddi;
	ddi.startTimestamp = pDevice->timestamps[0];
	ddi.endTimestamp = pDevice->timestamps[CACHE_ROW_NUM-1];
	ddi.dataOffset = dindex->dataOffset;
	ddi.dataLength = dindex->dataLength;
	ddi.totalFields = dictSize(pDevice->fields);
	strncpy(ddi.name,RedisModule_StringPtrLen(pDevice->keyName, NULL),MAX_NAME_SIZE-1);
	lseek(indexFd, 0, SEEK_END);
	write(indexFd, &ddi, sizeof(ddi));

    dictIterator *di = dictGetSafeIterator(pDevice->fields);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
    	FIELD *pField = dictGetVal(de);

    	FINDEX* findex = RedisModule_Alloc(sizeof(FINDEX));
    	findex->dataOffset = lseek(dataFd, 0, SEEK_END);

    	if(pField->type == FIELD_TYPE_BOOL){
    		int nelements = CACHE_BITMAP_SIZE * 3;
    		findex->dataLength = tsCompressTinyint((const char*)pField->cache,0,nelements,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
    	}else{
    		int field_size = getFieldTypeSize(pField);
    		int nelements = (CACHE_BITMAP_SIZE * 2)/field_size + CACHE_ROW_NUM;
    		findex->dataLength = gTaosCompFunc[pField->type]((const char*)pField->cache,0,nelements,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
    	}
    	write(dataFd, output, findex->dataLength);
    	RedisModule_DictSet(dindex->pFieldIndex,pField->keyName,findex);

    	DiskFINDEX dfi;
    	dfi.dataOffset = findex->dataOffset;
    	dfi.dataLength = findex->dataLength;
    	strncpy(dfi.name,RedisModule_StringPtrLen(pField->keyName, NULL),MAX_NAME_SIZE-1);
    	write(indexFd, &dfi, sizeof(dfi));

    	//memset(pField->cache,0,CACHE_BITMAP_SIZE*2);
    }
    dictReleaseIterator(di);

    RedisModule_Free(buffer);
    RedisModule_Free(output);
    close(dataFd);

    trimIndex(ctx,pDevice);
}

int add_sample(RedisModuleCtx *ctx,int rowId,FIELD *pField,const char* value){
	double last_value = 0;
/*	if(pField->flags & FIELD_FLAG_ACC){
		int m = CACHE_ROW_NUM-1;
		if(rowId > 0)
			m = rowId - 1;
		for(; m >= 0; m--){
			if(bitmapTestBit(pField->cache->haveTimestamp,m)){
				if(!bitmapTestBit(pField->cache->isValueNone,m)){
					last_value = pField->cache->dvalue[m];
					break;
				}
			}
		}
	}*/
	if(rowId > 0)
		last_value = pField->cache->dvalue[rowId-1];
	else
		last_value = pField->cache->dvalue[CACHE_ROW_NUM-1];

	if(strcmp(value,"none") == 0){
		bitmapSetBit(pField->cache->isValueNone,rowId);
	}else{
		if(pField->type == FIELD_TYPE_BOOL){
			if(strcmp(value,"1") == 0){
				bitmapSetBit(pField->cache->bvalue,rowId);
			}else if(strcmp(value,"0") == 0){
				bitmapClearBit(pField->cache->bvalue,rowId);
			}else{
				RedisModule_Log(ctx, "notice", "%s type change\n",RedisModule_StringPtrLen(pField->keyName,NULL));

				pField->type = FIELD_TYPE_DOUBLE;
				pField->typeChangeTimestamp = RedisModule_Milliseconds();

				double sample;
				if ((fast_double_parser_c_parse_number(value, &sample) == NULL)) {
					bitmapSetBit(pField->cache->isValueNone,rowId);
					return -201;
				}

				int cache_size = 2*CACHE_BITMAP_SIZE + sizeof(double) * CACHE_ROW_NUM;
				pField->cache = RedisModule_Realloc(pField->cache,cache_size);
				for(int m = rowId-1; m >= 0; m--){
					if(bitmapTestBit(pField->cache->haveTimestamp,m)){
						if(bitmapTestBit(pField->cache->bvalue,m)){
							pField->cache->dvalue[m] = 1;
						}else{
							pField->cache->dvalue[m] = 0;
						}
					}
				}
			}
		}else if(pField->type == FIELD_TYPE_DOUBLE){
			double sample;
			if ((fast_double_parser_c_parse_number(value, &sample) == NULL)) {
				bitmapSetBit(pField->cache->isValueNone,rowId);
			}else if(pField->flags & FIELD_FLAG_ACC){
				pField->cache->dvalue[rowId] = sample;
				if(last_value >= sample){
					bitmapSetBit(pField->cache->isValueNone,rowId);
				}
			}else{
				pField->cache->dvalue[rowId] = sample;
			}
		}
	}

	bitmapSetBit(pField->cache->haveTimestamp,rowId);

	return 0;
}

void replySubscribe(RedisModuleCtx *ctx,QFIELD* pFieldHeadTmp,int rowId){
	long long fieldlen = 0;
	RedisModule_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);
	while(pFieldHeadTmp){
		if(pFieldHeadTmp->curCache && bitmapTestBit(pFieldHeadTmp->curCache->haveTimestamp,rowId)){
			RedisModule_ReplyWithString(ctx,pFieldHeadTmp->pField->keyName);
			if(bitmapTestBit(pFieldHeadTmp->curCache->isValueNone,rowId)){
				RedisModule_ReplyWithCString(ctx,"none");
			}else{
				if(pFieldHeadTmp->pField->type == FIELD_TYPE_BOOL){
					if(bitmapTestBit(pFieldHeadTmp->curCache->bvalue,rowId)){
						RedisModule_ReplyWithBool(ctx,1);
					}else{
						RedisModule_ReplyWithBool(ctx,0);
					}
				}else{
					RedisModule_ReplyWithDouble(ctx,pFieldHeadTmp->curCache->dvalue[rowId]);
				}
			}
			fieldlen++;
		}
		pFieldHeadTmp = pFieldHeadTmp->next;
	}
	RedisModule_ReplySetMapLength(ctx, fieldlen);
}

void replyQuery(RedisModuleCtx *ctx,QFIELD* pFieldHeadTmp,int rowId){
	long long fieldlen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	while(pFieldHeadTmp){
		if(pFieldHeadTmp->curCache && bitmapTestBit(pFieldHeadTmp->curCache->haveTimestamp,rowId)){
			if(bitmapTestBit(pFieldHeadTmp->curCache->isValueNone,rowId)){
				RedisModule_ReplyWithCString(ctx,"none");
			}else{
				if(pFieldHeadTmp->pField->type == FIELD_TYPE_BOOL){
					if(bitmapTestBit(pFieldHeadTmp->curCache->bvalue,rowId)){
						RedisModule_ReplyWithBool(ctx,1);
					}else{
						RedisModule_ReplyWithBool(ctx,0);
					}
				}else{
					RedisModule_ReplyWithDouble(ctx,pFieldHeadTmp->curCache->dvalue[rowId]);
				}
			}
		}else{
			RedisModule_ReplyWithEmptyString(ctx);
		}
		pFieldHeadTmp = pFieldHeadTmp->next;
		fieldlen++;
	}
	RedisModule_ReplySetArrayLength(ctx, fieldlen);
}

int query_cahce(RedisModuleCtx *ctx,Device* pDevice,QFIELD* pFieldHead,int64_t startQueryTime, int64_t endQueryTime,ORDER_TYPE order,timestamp_t* curTimestamps,size_t curTotalSamples,QUERY_TYPE query_type){//2018-06-01 08:00:00.000
	int start,end,step;
	if(order == ORDER_ASC){
		start = 0;
		end = curTotalSamples;
		step = 1;
	}else{
		start = curTotalSamples-1;
		end = -1;
		step = -1;
	}
	lastSampleTimestamp = curTimestamps[curTotalSamples-1];

	long long samplelen = 0;

	for(int i = start; i != end; i+=step){
		if(curTimestamps[i] < startQueryTime){
			if(order == ORDER_DESC)
				break;
			else
				continue;
		}
		if(curTimestamps[i] > endQueryTime){
			if(order == ORDER_ASC)
				break;
			else
				continue;
		}

		if(query_type == QUERY_NORMAL){
			RedisModule_ReplyWithArray(ctx, 2);
			RedisModule_ReplyWithLongLong(ctx, curTimestamps[i]);
			replyQuery(ctx,pFieldHead,i);
			samplelen++;
		}else if(query_type == QUERY_SUB){
			RedisModule_ReplyWithArray(ctx, 2);
			//RedisModule_ReplyWithString(ctx, pDevice->keyName);
			RedisModule_ReplyWithLongLong(ctx, curTimestamps[i]);
			replySubscribe(ctx,pFieldHead,i);
			samplelen++;
		}else if(query_type == QUERY_AGG){
			long fieldlen = 0;

			QFIELD*pFieldHeadTmp = pFieldHead;
			while(pFieldHeadTmp){
				long agg_len = 0;

				if(pFieldHeadTmp->pField->type == FIELD_TYPE_BOOL){
					pFieldHeadTmp = pFieldHeadTmp->next;
					continue;
				}

				if(pFieldHeadTmp->curCache && bitmapTestBit(pFieldHeadTmp->curCache->haveTimestamp,i)){
					if(bitmapTestBit(pFieldHeadTmp->curCache->isValueNone,i)){
						pFieldHeadTmp = pFieldHeadTmp->next;
						continue;
					}

					CompactionRule *rule = pFieldHeadTmp->rules;
					while (rule != NULL) {
						double aggVal;
						if(handleQueryCompaction(rule,curTimestamps[i],pFieldHeadTmp->curCache->dvalue[i],&aggVal)){
							if(fieldlen == 0){
								RedisModule_ReplyWithArray(ctx, 2);
								RedisModule_ReplyWithLongLong(ctx, curTimestamps[i]);
								RedisModule_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);
							}
							if(agg_len == 0){
								RedisModule_ReplyWithString(ctx,pFieldHeadTmp->pField->keyName);
								RedisModule_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);
								fieldlen++;
							}
							RedisModule_ReplyWithCString(ctx,AggTypeEnumToString(rule->aggType));
							RedisModule_ReplyWithDouble(ctx,aggVal);
							agg_len++;

						}
						rule = rule->nextRule;
					}
					if(agg_len > 0){
						RedisModule_ReplySetMapLength(ctx, agg_len);
					}
				}
				pFieldHeadTmp = pFieldHeadTmp->next;
			}
			if(fieldlen > 0){
				RedisModule_ReplySetMapLength(ctx, fieldlen);
				samplelen++;
			}

		}

	}
	return samplelen;
}

int query_sample(RedisModuleCtx *ctx,Device* pDevice,QFIELD* pFieldHead,int64_t startQueryTime, int64_t endQueryTime,ORDER_TYPE order,QUERY_TYPE query_type){//2018-06-01 08:00:00.000
	timestamp_t* curTimestamps;
	size_t curTotalSamples;
	QFIELD* pFieldHeadTmp;

	long samplelen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	if(order == ORDER_DESC){
		curTimestamps = pDevice->timestamps;
		curTotalSamples = pDevice->totalSamples;
		QFIELD* pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			pFieldHeadTmp->curCache = pFieldHeadTmp->pField->cache;
			pFieldHeadTmp = pFieldHeadTmp->next;
		}
		samplelen += query_cahce(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,order,curTimestamps,curTotalSamples,query_type);
		pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			pFieldHeadTmp->curCache = NULL;
			pFieldHeadTmp = pFieldHeadTmp->next;
		}
	}

	timestamp_t rax_key;
	RedisModuleDictIter* dictIter;
	void *(*DictGetNext)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
    if (order == ORDER_ASC) {
        DictGetNext = RedisModule_DictNextC;
        encodeTimestamp(&rax_key, startQueryTime);
        dictIter = RedisModule_DictIteratorStartC(pDevice->pDeviceIndex, ">=", &rax_key, sizeof(rax_key));
    } else {
        DictGetNext = RedisModule_DictPrevC;
        encodeTimestamp(&rax_key, endQueryTime);
        dictIter = RedisModule_DictIteratorStartC(pDevice->pDeviceIndex, "<=", &rax_key, sizeof(rax_key));
    }

	timestamp_t *key;
	DINDEX *dindex;
	size_t keylen;
	int dataFd = 0;
	timestamp_t file_id = 0;

	int timestamp_output_size = sizeof(timestamp_t)*CACHE_ROW_NUM;
	int buffer_size = sizeof(timestamp_t)*CACHE_ROW_NUM;
	timestamp_t* timestamp_output = RedisModule_Alloc(timestamp_output_size);
	char* buffer = RedisModule_Alloc(buffer_size);
	while(1) {
		key = DictGetNext(dictIter,&keylen,(void**)&dindex);
		if(key == NULL){
			break;
		}

		RedisModule_Assert(keylen == sizeof(timestamp_t));
		timestamp_t endTimestamp = ntohu64(*key);
		if (order == ORDER_ASC){
			if(dindex->startTimestamp > endQueryTime)
				break;
		}else{
			if(endTimestamp < startQueryTime)
				break;
		}

		if(file_id != getFileId(endTimestamp)){
			file_id = getFileId(endTimestamp);
			char dbPathName[128];
			sprintf(dbPathName,"%s/%" PRIu64 ".data",RedisModule_StringPtrLen(edb_path, NULL),file_id);
			if(dataFd)
				close(dataFd);
			dataFd = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		lseek(dataFd, dindex->dataOffset,SEEK_SET);
		char*input = RedisModule_Alloc(dindex->dataLength);
		read(dataFd, input, dindex->dataLength);

		tsDecompressTimestamp(input, dindex->dataLength, CACHE_ROW_NUM, (char* const)timestamp_output,timestamp_output_size,TWO_STAGE_COMP,buffer,buffer_size);
		RedisModule_Free(input);

		pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			FINDEX* findex = RedisModule_DictGet(dindex->pFieldIndex,pFieldHeadTmp->pField->keyName,NULL);
			if(findex != NULL){
				lseek(dataFd, findex->dataOffset,SEEK_SET);
				char*input = RedisModule_Alloc(findex->dataLength);
				read(dataFd, input, findex->dataLength);

				pFieldHeadTmp->curCache = RedisModule_Alloc(sizeof(CACHE));
		    	if(pFieldHeadTmp->pField->type == FIELD_TYPE_BOOL){
		    		int nelements = CACHE_BITMAP_SIZE * 3;
		    		tsDecompressTinyint(input, findex->dataLength, nelements, (char* const)pFieldHeadTmp->curCache,sizeof(CACHE), TWO_STAGE_COMP, buffer, buffer_size);
		    	}else{
		    		int field_size = getFieldTypeSize(pFieldHeadTmp->pField);
		    		int nelements = (CACHE_BITMAP_SIZE * 2)/field_size + CACHE_ROW_NUM;
		    		gTaosDecompFunc[pFieldHeadTmp->pField->type](input, findex->dataLength, nelements, (char* const)pFieldHeadTmp->curCache,sizeof(CACHE), TWO_STAGE_COMP, buffer, buffer_size);
		    	}
		    	RedisModule_Free(input);
			}
			pFieldHeadTmp = pFieldHeadTmp->next;
		}

		curTimestamps = timestamp_output;
		curTotalSamples = CACHE_ROW_NUM;
		samplelen += query_cahce(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,order,curTimestamps,curTotalSamples,query_type);

		pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			if(pFieldHeadTmp->curCache){
				RedisModule_Free(pFieldHeadTmp->curCache);
				pFieldHeadTmp->curCache = NULL;
			}
			pFieldHeadTmp = pFieldHeadTmp->next;
		}
	}
	RedisModule_DictIteratorStop(dictIter);
	RedisModule_Free(buffer);
	RedisModule_Free(timestamp_output);

	if(order == ORDER_ASC){
		curTimestamps = pDevice->timestamps;
		curTotalSamples = pDevice->totalSamples;
		QFIELD* pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			pFieldHeadTmp->curCache = pFieldHeadTmp->pField->cache;
			pFieldHeadTmp = pFieldHeadTmp->next;
		}
		samplelen += query_cahce(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,order,curTimestamps,curTotalSamples,query_type);
		pFieldHeadTmp = pFieldHead;
		while(pFieldHeadTmp){
			pFieldHeadTmp->curCache = NULL;
			pFieldHeadTmp = pFieldHeadTmp->next;
		}
	}
	RedisModule_ReplySetArrayLength(ctx, samplelen);

	return samplelen;
}

void DeleteOldFile(RedisModuleCtx *ctx){
	RedisModule_Log(ctx, "notice", "[lmgw] deleteOldFile start!!!");

	DIR *dir = NULL;
	const char* cstr_edb_path = RedisModule_StringPtrLen(edb_path, NULL);
	if((dir = opendir(cstr_edb_path)) == NULL) {
		RedisModule_Log(ctx, "warning", "Failed to open edb_path. aborting...");
		return;
	}

	int64_t curFileId = getFileId(RedisModule_Milliseconds());

	struct dirent *file;
	char dbPathName[256];

	int64_t selectedTimeStamp[11];
	int curFileNum = 0;
	int maxFileNum = 10;
	off_t total_space = 0;
	while((file = readdir(dir))) {
		if (file->d_type != DT_REG)
			continue;

		int len = strlen(file->d_name);
		if(len < 10 || strcmp(file->d_name + (len - 6),".index") != 0)
			continue;

		char *endptr = NULL;
		int64_t endFileId = strtoll(file->d_name, &endptr, 10);
		if(retentionHours == 0 || (endFileId + retentionHours*EDB_FILE_PERIOD) > curFileId ){
			if(retentionMbs > 0){
				int j = curFileNum;
				while(j){
					if(endFileId < selectedTimeStamp[j-1]){
						if(j < maxFileNum)
							selectedTimeStamp[j] = selectedTimeStamp[j-1];
					}else{
						break;
					}
					j--;
				}

				selectedTimeStamp[j] = endFileId;
				if(curFileNum < maxFileNum)
					curFileNum++;
			}

			struct stat st;
			sprintf(dbPathName,"%s/%" PRIu64 ".index",cstr_edb_path,endFileId);
			stat(dbPathName, &st);
			total_space += st.st_size;
			sprintf(dbPathName,"%s/%" PRIu64 ".data",cstr_edb_path,endFileId);
			stat(dbPathName, &st);
			total_space += st.st_size;
			continue;
		}

		if(lastDeleteFileId < endFileId)
			lastDeleteFileId = endFileId;
		sprintf(dbPathName,"%s/%" PRIu64 ".index",cstr_edb_path,endFileId);
		unlink(dbPathName);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",cstr_edb_path,endFileId);
		unlink(dbPathName);

		RedisModule_Log(ctx, "notice", "[lmgw] delete Old File : %s!!!",dbPathName);
	}
	closedir(dir);

	for(int i = 0; i < curFileNum; i++){
		if(total_space < retentionMbs*1024*1024)
			break;

		struct stat st;
		sprintf(dbPathName,"%s/%" PRIu64 ".index",cstr_edb_path,selectedTimeStamp[i]);
		stat(dbPathName, &st);
		total_space -= st.st_size;
		unlink(dbPathName);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",cstr_edb_path,selectedTimeStamp[i]);
		stat(dbPathName, &st);
		total_space -= st.st_size;
		unlink(dbPathName);
		RedisModule_Log(ctx, "notice", "[lmgw] delete Old File : %s!!!",dbPathName);

		if(lastDeleteFileId < selectedTimeStamp[i])
			lastDeleteFileId = selectedTimeStamp[i];
	}
}

void LoadCache(RedisModuleCtx *ctx){
	const char* cstr_edb_path = RedisModule_StringPtrLen(edb_path, NULL);
	DIR *dir = NULL;
	if((dir = opendir(cstr_edb_path)) == NULL) {
		RedisModule_Log(ctx, "warning", "Failed to open edb_path. aborting...");
		return;
	}

	struct dirent *file;
	while((file = readdir(dir))) {
		if (file->d_type != DT_REG)
			continue;

		int len = strlen(file->d_name);
		if(len < 10 || strcmp(file->d_name + (len - 6),".index") != 0)
			continue;

		char dbPathName[128];
		snprintf(dbPathName,127,"%s/%s",cstr_edb_path,file->d_name);
		int indexFd = open(dbPathName, O_RDWR);
		DiskDINDEX ddi;
		while(1){
			ssize_t s = read(indexFd,&ddi,sizeof(ddi));
			if(s <= 0)
				break;
			RedisModuleString *device_name = RedisModule_CreateString(ctx,ddi.name,strlen(ddi.name));
			RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

			int keytype = RedisModule_KeyType(device_key);
			if (keytype != REDISMODULE_KEYTYPE_MODULE || keytype == REDISMODULE_KEYTYPE_EMPTY) {
				RedisModule_Log(ctx, "warning", "Failed to open device[%s]. aborting...",ddi.name);
				lseek(indexFd,ddi.totalFields*sizeof(DiskFINDEX),SEEK_CUR);
				continue;
			}

			Device* pDevice;
			if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
				RedisModule_Log(ctx, "warning", "Failed to open device[%s]. aborting...",ddi.name);
				lseek(indexFd,ddi.totalFields*sizeof(DiskFINDEX),SEEK_CUR);
				continue;
			}else{
				pDevice = RedisModule_ModuleTypeGetValue(device_key);
			}
			DINDEX* dindex = RedisModule_Alloc(sizeof(DINDEX));
			dindex->pFieldIndex = RedisModule_CreateDict(NULL);
			dindex->startTimestamp = ddi.startTimestamp;
			dindex->dataOffset = ddi.dataOffset;
			dindex->dataLength = ddi.dataLength;
			dictOperator(pDevice->pDeviceIndex,dindex,ddi.endTimestamp,DICT_OP_SET);
			for(int i = 0; i < ddi.totalFields; i++){
				DiskFINDEX dfi;
				read(indexFd,&dfi,sizeof(dfi));
				FINDEX* findex = RedisModule_Alloc(sizeof(FINDEX));
				findex->dataOffset = dfi.dataOffset;
				findex->dataLength = dfi.dataLength;
				RedisModule_DictSetC(dindex->pFieldIndex,dfi.name,strlen(dfi.name),findex);
			}
			RedisModule_FreeString(ctx,device_name);
		}

	}

}

void IndexDeviceFromName(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    // Try to open the series
    Device *pDevice;
    RedisModuleKey *key = NULL;
    RedisModuleString *_keyname = RedisModule_HoldString(ctx, keyname);
    const int status = GetDevice(ctx, _keyname, &key, &pDevice, REDISMODULE_READ, false, true);
    if (!status) { // Not a device key
        goto cleanup;
    }

    if (unlikely(IsKeyIndexed(_keyname))) {
        // when loading from rdb file the key shouldn't exist.
        size_t len;
        const char *str = RedisModule_StringPtrLen(_keyname, &len);
        RedisModule_Log(
            ctx, "warning", "Trying to load rdb a key=%s, which is already in index", str);
        RemoveIndexedMetric(_keyname); // for safety
    }

    IndexMetric(_keyname, pDevice->labels, pDevice->labelsCount);

cleanup:
    if (key) {
        RedisModule_CloseKey(key);
    }
    RedisModule_FreeString(ctx, _keyname);
}
