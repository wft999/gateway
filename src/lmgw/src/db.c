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
#include <unistd.h>
#include <sys/time.h>
#include <stdint.h>
#include <dirent.h>
#include <math.h>
#include <assert.h>
#include <inttypes.h>

#include "db.h"
#include "fast_double_parser_c.h"
#include "lzf.h"
#include "ttime.h"

int64_t gMillisecondOfHour = 60 * 60 * 1000;
char* field_type_name[] = {"FIELD_TYPE_BOOL","FIELD_TYPE_DOUBLE"};
const unsigned char true_bits[8] = {(1<<0),(1<<1),(1<<2),(1<<3),(1<<4),(1<<5),(1<<6),(1<<7)};
const unsigned char false_bits[8] = {~(1<<0),~(1<<1),~(1<<2),~(1<<3),~(1<<4),~(1<<5),~(1<<6),~(1<<7)};
const int cache_head_size = CACHE_BITMAP_SIZE*2 + sizeof(CompressedChunk);

DB* gdb = NULL;
const double g_bad_number = -99999;
void *saveBackgroundJob(void *arg);

int db_ready() {
    return gdb->dbReady == 1;
}

void FreeCompactionRule(void *value) {
    CompactionRule *rule = (CompactionRule *)value;
    RedisModule_FreeString(rts_staticCtx,rule->destKey);
    ((AggregationClass *)rule->aggClass)->freeContext(rule->aggContext);
    free(rule);
}

uint64_t distCStrHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}
int distCStrKeyCompare(dict *d, const void *key1, const void *key2) {
    int l1,l2;
    UNUSED(d);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}
void dictFieldNameDestructor(dict *d, void *val)
{
    UNUSED(d);
    RedisModule_Free(val);
}
void dictFieldDestructor(dict *d, void *val)
{
    UNUSED(d);
    FIELD* pField = (FIELD*)val;
    RedisModule_FreeString(rts_staticCtx,pField->func);
    RedisModule_FreeString(rts_staticCtx,pField->args);

    CompactionRule *rule = pField->rules;
	while (rule != NULL) {
		CompactionRule *nextRule = rule->nextRule;
		FreeCompactionRule(rule);
		rule = nextRule;
	}

	RedisModule_Free(pField->cache[0]);
	RedisModule_Free(pField->cache[1]);
    RedisModule_Free(val);
}
dictType fieldsDictType = {
	distCStrHash,            	/* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
	distCStrKeyCompare,      	/* key compare */
	dictFieldNameDestructor,    /* key destructor */
	dictFieldDestructor,        /* val destructor */
    NULL                        /* allow to expand */
};



int get_row_id(int64_t timeStamp_sec){
	if(gdb->startTimestamp == 0){
		gdb->startTimestamp = timeStamp_sec;
	}
	int rowId = timeStamp_sec - gdb->startTimestamp;
	gdb->curRowNum = (rowId+1)>gdb->curRowNum?(rowId+1):gdb->curRowNum;
	if(rowId >= CACHE_ROW_NUM){
		rowId = 0;
		gdb->preStartTimestamp = gdb->startTimestamp;
		gdb->startTimestamp = timeStamp_sec;
		gdb->curRowNum = 0;

		pthread_attr_t attr;
		pthread_t thread;

		pthread_attr_init(&attr);
		void *arg = (void*)gdb->cacheId;
		if(pthread_create(&thread,&attr,saveBackgroundJob,arg) != 0){
			return -201;
		}
		pthread_detach(thread);

		gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;
	}

	return rowId;
}

void sync_stream(RedisModuleCtx *ctx,RedisModuleString *stream_name,RedisModuleString *stream_id){
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, stream_name, REDISMODULE_READ);
	int type = RedisModule_KeyType(key);
	if (type != REDISMODULE_KEYTYPE_STREAM) {
		return;
	}

	RedisModuleStreamID start;
	start.ms = 0;
	start.seq = 0;
	if(stream_id != NULL && RedisModule_StringToStreamID(stream_id,&start) == REDISMODULE_ERR){
		return;
	}
	const char* device = RedisModule_StringPtrLen(stream_name,NULL) + strlen("sample:");

	RedisModule_StreamIteratorStart(key, REDISMODULE_STREAM_ITERATOR_EXCLUSIVE, &start, NULL);
	RedisModuleStreamID id;
	long numfields;
	while (RedisModule_StreamIteratorNextID(key, &id, &numfields) ==
	       REDISMODULE_OK) {

		int rowId = get_row_id(id.ms / 1000);

	    RedisModuleString *field_str, *value_str;
	    while (RedisModule_StreamIteratorNextField(key, &field_str, &value_str) ==
	           REDISMODULE_OK) {
	    	const char* field = RedisModule_StringPtrLen(field_str,NULL);
	    	const char* value = RedisModule_StringPtrLen(value_str,NULL);
	    	FIELD *pField = get_field(ctx,device,field);
	    	if(pField == NULL)
	    		continue;
	        put_db(ctx,rowId,pField,value);
	        RedisModule_FreeString(ctx, field_str);
	        RedisModule_FreeString(ctx, value_str);
	    }
	}
	RedisModule_StreamIteratorStop(key);
	RedisModule_CloseKey(key);
}

void sync_db(RedisModuleCtx *ctx){
	RedisModule_AutoMemory(ctx);

	RedisModuleString* key_name = RedisModule_CreateString(ctx,"node:lmgw",strlen("node:lmgw"));
	RedisModuleKey *lmgw_key = RedisModule_OpenKey(ctx, key_name, REDISMODULE_READ);
	int key_type = RedisModule_KeyType(lmgw_key);
	if(REDISMODULE_KEYTYPE_EMPTY != key_type){
		RedisModule_Assert(key_type == REDISMODULE_KEYTYPE_HASH);

		RedisModuleCallReply *reply = RedisModule_Call(ctx, "KEYS", "c", "sample:*");
		size_t len = RedisModule_CallReplyLength(reply);
		for (size_t i = 0; i < len; i++) {
			RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(reply, i);
			RedisModuleString* stream_name = RedisModule_CreateStringFromCallReply(keyReply);

			RedisModuleString *stream_id;
			RedisModule_HashGet(lmgw_key, REDISMODULE_HASH_NONE, stream_name, &stream_id,NULL);
			sync_stream(ctx,stream_name,stream_id);
			RedisModule_FreeString(ctx,stream_name);
			RedisModule_FreeString(ctx,stream_id);
		}
		RedisModule_FreeCallReply(reply);
	}

	RedisModule_FreeString(ctx,key_name);
	RedisModule_CloseKey(lmgw_key);
}

CompactionRule *NewRule(RedisModuleString *destKey,int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment) {
    if (bucketDuration == 0ULL) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext(false);
    rule->bucketDuration = bucketDuration;
    rule->timestampAlignment = timestampAlignment;
    rule->destKey = destKey;
    rule->startCurrentTimeBucket = -1LL;
    rule->nextRule = NULL;

    return rule;
}

CompactionRule *add_rule(RedisModuleCtx *ctx,
								FIELD *pField,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment) {
	const char* type_name = AggTypeEnumToStringLowerCase(aggType);
	RedisModuleString *destKey = RedisModule_CreateStringPrintf(ctx,"%s:%s:%" PRIu64 "", pField->name,type_name,bucketDuration);
    CompactionRule *rule =
        NewRule(destKey,aggType, bucketDuration, timestampAlignment);
    if (rule == NULL) {
    	RedisModule_FreeString(ctx,destKey);
        return NULL;
    }

    RedisModule_RetainString(ctx, destKey);
    if (pField->rules == NULL) {
    	pField->rules = rule;
    } else {
        CompactionRule *last = pField->rules;
        while (last->nextRule != NULL)
            last = last->nextRule;
        last->nextRule = rule;
    }
    return rule;
}

void* add_field(RedisModuleCtx *ctx,char *field_name,FIELD_TYPE type){
	FIELD* pField = (FIELD*)RedisModule_Alloc(sizeof(FIELD));
	if(pField == NULL)
		return NULL;

	memset(pField,0,sizeof(FIELD));
	pField->name = RedisModule_Strdup(field_name);

	int retval = dictAdd(gdb->fields, pField->name, pField);
	if(retval != 0){
		RedisModule_Free(pField->name);
		RedisModule_Free(pField);
		return NULL;
	}

	pField->type = type;
	if(pField->type == FIELD_TYPE_DOUBLE){
		int size = cache_head_size + CHUNK_DEFAULT_SIZE;
		pField->cache[0] = RedisModule_Calloc(1,size);
		pField->cache[0]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[0]->chunk.prevLeading = 32;
		pField->cache[0]->chunk.prevTrailing = 32;

		pField->cache[1] = RedisModule_Calloc(1,size);
		pField->cache[1]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[1]->chunk.prevLeading = 32;
		pField->cache[1]->chunk.prevTrailing = 32;
		RedisModule_Call(ctx, "HSET", "!ccc", field_name,"type","double");
	}else if(type == FIELD_TYPE_BOOL){
		int size = sizeof(CACHE);
		pField->cache[0] = RedisModule_Calloc(1,size);
		pField->cache[1] = RedisModule_Calloc(1,size);
		RedisModule_Call(ctx, "HSET", "!ccc", field_name,"type","bool");
	}

	return pField;
}

void* get_field(RedisModuleCtx *ctx,const char* device, const char* field){
	char strFieldName[128];
	sprintf(strFieldName,"tag:%s:%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		pField = add_field(ctx,strFieldName,FIELD_TYPE_BOOL);
	}
	return pField;
}

void init_aggs(RedisModuleCtx *ctx,FIELD* pField, RedisModuleString *agg_names){
	size_t len;
	const char *aggs = RedisModule_StringPtrLen(agg_names, NULL);
	while(1){
		const char *next = strchr(aggs,':');
		if(!next){
			RedisModule_Log(ctx, "warning", "Unable to parse aggregation of %s",pField->name);
			break;
		}

		len = next - aggs;
		TS_AGG_TYPES_T aggType = StringLenAggTypeToEnum(aggs,len);
		if(TS_AGG_INVALID == aggType){
			RedisModule_Log(ctx, "warning", "Unable to parse aggregation of %s",pField->name);
			break;
		}

		aggs = next + 1;
		char *endptr = NULL;
		timestamp_t bucketDuration= strtoll(aggs, &endptr, 10);
		if(endptr == aggs || bucketDuration == LLONG_MAX || bucketDuration == LLONG_MIN){
			RedisModule_Log(ctx, "warning", "Unable to parse aggregation of tag:%s",pField->name);
			break;
		}

		if(endptr && endptr[0] == ','){
			aggs = endptr + 1;
		}else{
			RedisModule_Log(ctx, "warning", "Unable to parse aggregation of tag:%s",pField->name);
			break;
		}

		timestamp_t timestampAlignment= strtoll(aggs, &endptr, 10);
		if(endptr == aggs || timestampAlignment == LLONG_MAX || timestampAlignment == LLONG_MIN){
			RedisModule_Log(ctx, "warning", "Unable to parse aggregation of tag:%s",pField->name);
			break;
		}

		add_rule(ctx,pField,aggType,bucketDuration*1000,timestampAlignment*1000);
		if(endptr && endptr[0] == ';'){
			aggs = endptr + 1;
		}else{
			break;
		}
	}
}

FIELD* init_field(RedisModuleCtx *ctx,RedisModuleString *field_name){
	RedisModuleKey *tag_key = RedisModule_OpenKey(ctx, field_name, REDISMODULE_READ);
	int type = RedisModule_KeyType(tag_key);
	if (type != REDISMODULE_KEYTYPE_HASH) {
		RedisModule_CloseKey(tag_key);
		return NULL;
	}

	FIELD* pField = (FIELD*)RedisModule_Alloc(sizeof(FIELD));
	if(pField == NULL)
		return NULL;

	memset(pField,0,sizeof(FIELD));
	pField->name = RedisModule_Strdup(RedisModule_StringPtrLen(field_name,NULL));

	int retval = dictAdd(gdb->fields, pField->name, pField);
	if(retval != 0){
		RedisModule_Free(pField->name);
		RedisModule_Free(pField);
		return NULL;
	}

	RedisModuleString *data_type,*agg_names,*typeChangeTimestamp;
	RedisModule_HashGet(tag_key, REDISMODULE_HASH_CFIELDS,"type",&data_type, "function", &pField->func,
								"arguments", &pField->args,"aggregation",&agg_names,"typeChangeTimestamp",&typeChangeTimestamp, NULL);
	RedisModule_CloseKey(tag_key);
	if(agg_names){
		init_aggs(ctx,pField,agg_names);
		RedisModule_FreeString(ctx,agg_names);
	}

	pField->typeChangeTimestamp = 0;
	if(typeChangeTimestamp != NULL){
		RedisModule_StringToLongLong(typeChangeTimestamp,&pField->typeChangeTimestamp);
		RedisModule_FreeString(ctx,typeChangeTimestamp);
	}

	pField->type = FIELD_TYPE_DOUBLE;
	if(data_type != NULL){
		const char* str = RedisModule_StringPtrLen(data_type,NULL);
		if(strcmp(str,"bool") == 0){
			type = FIELD_TYPE_BOOL;
		}else if(strcmp(str,"double") == 0){
			type = FIELD_TYPE_DOUBLE;
		}
		RedisModule_FreeString(ctx,data_type);
	}
	if(pField->type == FIELD_TYPE_DOUBLE){
		int size = cache_head_size + CHUNK_DEFAULT_SIZE;
		pField->cache[0] = RedisModule_Calloc(1,size);
		pField->cache[0]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[0]->chunk.prevLeading = 32;
		pField->cache[0]->chunk.prevTrailing = 32;

		pField->cache[1] = RedisModule_Calloc(1,size);
		pField->cache[1]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[1]->chunk.prevLeading = 32;
		pField->cache[1]->chunk.prevTrailing = 32;
	}else if(type == FIELD_TYPE_BOOL){
		int size = sizeof(CACHE);
		pField->cache[0] = RedisModule_Calloc(1,size);
		pField->cache[1] = RedisModule_Calloc(1,size);
	}

	return pField;
}

int init_dict(RedisModuleCtx *ctx){
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "KEYS", "c", "tag:*");
	RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
	size_t len = RedisModule_CallReplyLength(reply);
	for (size_t i = 0; i < len; i++) {
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(reply, i);
		RedisModuleString* field_name = RedisModule_CreateStringFromCallReply(keyReply);
		init_field(ctx,field_name);
		RedisModule_FreeString(ctx,field_name);
	}
	RedisModule_FreeCallReply(reply);

	return 0;
}

int open_db(RedisModuleCtx *ctx){
	if(gdb->dbReady == 1)
		return 0;

	int ret = init_dict(ctx);
	if(ret < 0){
		close_db();
		return ret;
	}
	sync_db(ctx);

	gdb->dbReady = 1;
	return 0;
}

int init_db(RedisModuleCtx *ctx,const char* path,unsigned int retentionHours,unsigned int retentionMbs){
	if(strlen(path) > (sizeof(gdb->path)-1))
		return -1;

	struct stat st;
	if (stat(path, &st) < 0){
		return -1;
	}

	gdb = RedisModule_Alloc(sizeof(DB));
	if(gdb == NULL){
		return -2;
	}

	memset(gdb, 0, sizeof(DB));
	gdb->signature = gdb;
	strcpy(gdb->path,path);
	//gdb->gCompressType = COMPRESS_TAOS_TWO_STEP;
	gdb->retentionHours = (retentionHours > RETENTION_HOURS_MAX)?RETENTION_HOURS_MAX:retentionHours;
	gdb->retentionMbs = (retentionMbs > RETENTION_MBS_MAX)?RETENTION_MBS_MAX:retentionMbs;

	pthread_mutex_init(&(gdb->dmutex), NULL);

	gdb->fields = dictCreate(&fieldsDictType);

	return 0;
}

void close_db(){
	if(gdb && gdb->signature == gdb){
		pthread_mutex_lock(&gdb->dmutex);
		if(gdb->startTimestamp){
			gdb->preStartTimestamp = gdb->startTimestamp;
			saveBackgroundJob((void*)gdb->cacheId);
		}

		if(gdb->fields) dictRelease(gdb->fields);
		if(gdb->dataFd) close(gdb->dataFd);

		pthread_mutex_unlock(&gdb->dmutex);
		pthread_mutex_destroy(&(gdb->dmutex));
		RedisModule_Free(gdb);
		gdb = NULL;
	}
}

void delete_index(int64_t maxEndTimeStamp){
	RedisModule_ThreadSafeContextLock(rts_staticCtx);
	//RedisModule_AutoMemory(rts_staticCtx);

	RedisModuleCallReply *reply = RedisModule_Call(rts_staticCtx, "KEYS", "c", "index:*");
	RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
	size_t len = RedisModule_CallReplyLength(reply);
	for (size_t i = 0; i < len; i++) {
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(reply, i);
		RedisModuleString* stream_name = RedisModule_CreateStringFromCallReply(keyReply);
		RedisModuleStreamID id;
		id.ms = maxEndTimeStamp;
		id.seq = 0;
		RedisModuleString *stream_id = RedisModule_CreateStringFromStreamID(rts_staticCtx, &id);
		RedisModuleCallReply *reply2 = RedisModule_Call(rts_staticCtx, "XTRIM", "!scs", stream_name ,"MINID",stream_id);
		RedisModule_FreeCallReply(reply2);
		RedisModule_FreeString(rts_staticCtx,stream_name);
		RedisModule_FreeString(rts_staticCtx,stream_id);
	}
	RedisModule_FreeCallReply(reply);

	RedisModule_ThreadSafeContextUnlock(rts_staticCtx);
}

void deleteOldFile(){
	DIR *dir = NULL;
	struct dirent *file;

	if((dir = opendir(gdb->path)) == NULL) {
		RedisModule_Log(rts_staticCtx, "warning", "deleteOldFile opendir failed!!!!");
		return;
	}

	char dbPathName[256];
	int64_t maxEndTimeStamp = 0;

	int64_t selectedTimeStamp[11];
	int curFileNum = 0;
	int maxFileNum = 10;
	off_t total_space = 0;
	while((file = readdir(dir))) {
		if (file->d_type != DT_REG)
			continue;

		int len = strlen(file->d_name);
		if(len < 10 || strcmp(file->d_name + (len - 5),".data") != 0)
			continue;

		char *endptr = NULL;
		int64_t startTimeStamp = strtoll(file->d_name, &endptr, 10);
		int64_t endTimeStamp = startTimeStamp * 3600 + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
		if(gdb->retentionHours == 0 || (gdb->startTimestamp - endTimeStamp) < gdb->retentionHours * 3600){
			if(gdb->retentionMbs > 0){
				int j = curFileNum;
				while(j){
					if(startTimeStamp < selectedTimeStamp[j-1]){
						if(j < maxFileNum)
							selectedTimeStamp[j] = selectedTimeStamp[j-1];
					}else{
						break;
					}
					j--;
				}

				selectedTimeStamp[j] = startTimeStamp;
				if(curFileNum < maxFileNum)
					curFileNum++;
			}

			struct stat st;
			sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->path,startTimeStamp);
			stat(dbPathName, &st);
			total_space += st.st_size;
			continue;
		}

		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->path,startTimeStamp);
		unlink(dbPathName);
		if(maxEndTimeStamp < endTimeStamp)
			maxEndTimeStamp = endTimeStamp;
	}
	closedir(dir);

	for(int i = 0; i < curFileNum; i++){
		if(total_space < gdb->retentionMbs*1024*1024)
			break;

		struct stat st;
		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->path,selectedTimeStamp[i]);
		stat(dbPathName, &st);
		total_space -= st.st_size;
		unlink(dbPathName);

		int64_t endTimeStamp = selectedTimeStamp[i] + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
		if(maxEndTimeStamp < endTimeStamp)
			maxEndTimeStamp = endTimeStamp;
	}
	if(maxEndTimeStamp > 0)
		delete_index(maxEndTimeStamp*1000);

}

void write_index(FIELD *pField,INDEX *pIndex){
	RedisModule_ThreadSafeContextLock(rts_staticCtx);
	//RedisModule_AutoMemory(rts_staticCtx);
	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(rts_staticCtx,"index:%s", pField->name);
	RedisModuleStreamID id;
	id.ms = gdb->preStartTimestamp*1000;
	id.seq = 0;
	RedisModuleString *stream_id = RedisModule_CreateStringFromStreamID(rts_staticCtx, &id);
	RedisModuleCallReply *reply = RedisModule_Call(rts_staticCtx, "XADD", "!ssclcl", stream_name,stream_id ,"Offset", pIndex->dataOffset,"length", pIndex->dataLength);
	RedisModule_FreeString(rts_staticCtx,stream_name);
	RedisModule_FreeString(rts_staticCtx,stream_id);
	RedisModule_FreeCallReply(reply);
	RedisModule_ThreadSafeContextUnlock(rts_staticCtx);
}

void *saveBackgroundJob(void *arg) {
	if(gdb == NULL || gdb->preStartTimestamp == 0)
		return NULL;

	RedisModule_Log(rts_staticCtx, "notice", "edb start save!!!");
	unsigned long cacheId = (unsigned long)arg;

	int output_size = sizeof(double)*CACHE_ROW_NUM;
	char* output = RedisModule_Alloc(output_size);

	char buf[256];
	if(!gdb->dataFd){
		sprintf(buf,"%s/%" PRIu64 ".data",gdb->path,gdb->preStartTimestamp/3600);
		gdb->dataFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
		gdb->fileStartTimestamp = gdb->preStartTimestamp;
	}

    dictIterator *di = dictGetSafeIterator(gdb->fields);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
    	FIELD *pField = dictGetVal(de);
    	INDEX index;
    	index.dataOffset = lseek(gdb->dataFd, 0, SEEK_END);
    	CACHE* pCache = pField->cache[cacheId];
		if(pField->type == FIELD_TYPE_BOOL){
			int nelements = CACHE_BITMAP_SIZE * 3;
			index.dataLength = lzf_compress((char*)pCache, nelements, output, output_size);
			write(gdb->dataFd, output, index.dataLength);

			memset(pCache,0,sizeof(CACHE));
		}else{
			int nelements = cache_head_size + pCache->chunk.idx/8 + (pCache->chunk.idx%8?1:0);
			if(output_size < (nelements + 1)){
				output_size = nelements + 1;
				output = RedisModule_Realloc(output,output_size);
			}
			index.dataLength = lzf_compress(pCache, nelements, output, output_size);
			write(gdb->dataFd, output, index.dataLength);

			int chunk_size = pCache->chunk.size;
			memset(pCache,0,cache_head_size + chunk_size);
			pCache->chunk.size = chunk_size;
			pCache->chunk.prevLeading = 32;
			pCache->chunk.prevTrailing = 32;
		}
		write_index(pField,&index);
    }
    dictReleaseIterator(di);
	RedisModule_Free(output);

	int64_t fileEndTimeStamp = gdb->fileStartTimestamp + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
	if(gdb->startTimestamp > fileEndTimeStamp){
		close(gdb->dataFd);
		gdb->dataFd = 0;
		if(gdb->retentionHours > 0 || gdb->retentionMbs > 0)
			deleteOldFile();
	}

	RedisModule_ThreadSafeContextLock(rts_staticCtx);
	RedisModuleStreamID id;
	id.ms = fileEndTimeStamp*1000;
	id.seq = 0;
	RedisModuleString *stream_id = RedisModule_CreateStringFromStreamID(rts_staticCtx,&id);
	RedisModuleCallReply *reply = RedisModule_Call(rts_staticCtx, "HSET", "!ccs", "node:lmgw","sample:*",stream_id);
	RedisModule_FreeString(rts_staticCtx,stream_id);
	RedisModule_FreeCallReply(reply);
	RedisModule_ThreadSafeContextUnlock(rts_staticCtx);

	gdb->preStartTimestamp = 0;
	RedisModule_Log(rts_staticCtx, "notice", "edb finish save!!!");
	return NULL;
}

static void ensureAddSample(FIELD* pField,int cacheId, double sample) {
    ChunkResult res = Compressed_Append(&pField->cache[cacheId]->chunk, sample);
    if (res != CR_OK) {
        int oldsize = pField->cache[cacheId]->chunk.size;
        pField->cache[cacheId]->chunk.size += CHUNK_RESIZE_STEP;
        int newsize = pField->cache[cacheId]->chunk.size;
        pField->cache[cacheId] = RedisModule_Realloc(pField->cache[cacheId], cache_head_size + newsize * sizeof(char));
        memset((char *)(pField->cache[cacheId]->chunk.data) + oldsize, 0, CHUNK_RESIZE_STEP);
        printf("%s cache%d extended to %lu \n", pField->name,cacheId,pField->cache[cacheId]->chunk.size);
        res = Compressed_Append(&pField->cache[cacheId]->chunk,sample);
        assert(res == CR_OK);
    }
}

int put_db(RedisModuleCtx *ctx,int rowId,FIELD *pField,const char* value){
	if(gdb == NULL || gdb->dbReady == 0)
		return -200;

	pthread_mutex_lock(&gdb->dmutex);
	int cacheId = gdb->cacheId;

	CACHE* pCache = pField->cache[cacheId];
	pCache->haveTimestamp[rowId/8] += 1 << (rowId%8);
	if(strcmp(value,"none") == 0){
		pCache->isValueNone[rowId/8] += 1 << (rowId%8);
	}else{
		if(pField->type == FIELD_TYPE_BOOL){
			if(strcmp(value,"1") == 0){
				pCache->bvalue[rowId/8] |= true_bits[rowId%8];
			}else if(strcmp(value,"0") == 0){
				pCache->bvalue[rowId/8] &= false_bits[rowId%8];
			}else{
				//printf("%s type change\n",pField->name);
				pField->type = FIELD_TYPE_DOUBLE;
				pField->typeChangeTimestamp = gdb->startTimestamp;
				//RedisModuleString *tag_name = RedisModule_CreateStringPrintf(ctx,"tag:%s", pField->name);
				RedisModuleCallReply *reply = RedisModule_Call(ctx, "HSET", "!ccccl", pField->name,"type","double","typeChangeTimestamp",gdb->startTimestamp);
				RedisModule_FreeCallReply(reply);
				//RedisModule_FreeString(ctx,tag_name);

				double sample;
				if ((fast_double_parser_c_parse_number(value, &sample) == NULL)) {
						pthread_mutex_unlock(&gdb->dmutex);
						return -201;
				}

				int size = cache_head_size + CHUNK_DEFAULT_SIZE;
				pField->cache[cacheId] = RedisModule_Calloc(size,1);
				pField->cache[cacheId]->chunk.size = CHUNK_DEFAULT_SIZE;
				pField->cache[cacheId]->chunk.prevLeading = 32;
				pField->cache[cacheId]->chunk.prevTrailing = 32;
				for(int m = 0; m < rowId; m++){
					if(pCache->haveTimestamp[m/8] & true_bits[m%8]){
						if(pCache->bvalue[m/8] & true_bits[m%8]){
							ensureAddSample(pField,cacheId,1);
						}else{
							ensureAddSample(pField,cacheId,0);
						}
					}
				}
				RedisModule_Free(pCache);
				ensureAddSample(pField,cacheId,sample);
			}
		}else if(pField->type == FIELD_TYPE_DOUBLE){
			if(pCache->chunk.size == 0){
				RedisModule_Free(pCache);
				int size = cache_head_size + CHUNK_DEFAULT_SIZE;
				pField->cache[cacheId] = RedisModule_Calloc(size,1);
				pField->cache[cacheId]->chunk.size = CHUNK_DEFAULT_SIZE;
				pField->cache[cacheId]->chunk.prevLeading = 32;
				pField->cache[cacheId]->chunk.prevTrailing = 32;
			}

			double sample;
			if ((fast_double_parser_c_parse_number(value, &sample) == NULL)) {
				pthread_mutex_unlock(&gdb->dmutex);
				return -201;
			}

			ensureAddSample(pField,cacheId,sample);
		}
	}

	pthread_mutex_unlock(&gdb->dmutex);
	return 0;
}

int dump(char* strFileName,char* device,char* field){
	if(gdb == NULL)
		return -300;
/*
	char strFieldName[128];
	sprintf(strFieldName,"%s.%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		printf("field not exist!");
	}else{
		char dbPathName[256];
		sprintf(dbPathName,"%s/%s.index",gdb->path,strFileName);
		int indexFd = open(dbPathName, O_RDWR);
		sprintf(dbPathName,"%s/%s.data",gdb->path,strFileName);
		int dataFd = open(dbPathName, O_RDWR);

		BLOCK block;
		read(indexFd, &block, sizeof(block));
		lseek(indexFd, pField->id * sizeof(INDEX), SEEK_CUR);
		INDEX index;
		read(indexFd, &index, sizeof(index));
		close(indexFd);
		lseek(dataFd, index.dataOffset,SEEK_SET);

		char*input = RedisModule_Alloc(index.dataLength);
		read(dataFd, input, index.dataLength);
		close(dataFd);

		int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
		char* output = RedisModule_Alloc(output_size+1);
		//tsDecompressStringImp(input, index.dataLength, output,output_size);
		lzf_decompress(input, index.dataLength, output,output_size);

		CACHE* pCache = (CACHE*)output;
		ChunkIter_t *iter = NULL;
		if(pField->type == FIELD_TYPE_DOUBLE){
			iter = Compressed_NewChunkIterator(&pCache->chunk);
		}
	    for (int i = 0; i < CACHE_ROW_NUM; ++i) {
	    	if(!(pCache->haveTimestamp[i/8] & true_bits[i%8]))
	    		continue;
	    	if(pCache->isValueNone[i/8] & true_bits[i%8]){
	    		printf("None\n");
	    		continue;
	    	}

			if(pField->type == FIELD_TYPE_BOOL){
				if(pCache->bvalue[i/8] & true_bits[i%8])
					printf("On\n");
				else
					printf("Off\n");
			}else if(pField->type == FIELD_TYPE_DOUBLE){
				double sample;
				Compressed_ChunkIteratorGetNext(iter, &sample);
				printf("%f\n",sample);
			}
	    }
		if(pField->type == FIELD_TYPE_DOUBLE){
			Compressed_FreeChunkIterator(iter);
		}


		RedisModule_Free(input);
		RedisModule_Free(output);
	}*/

	return 0;
}

int query_block(RedisModuleCtx *ctx,FIELD *pField,CACHE* pCache,int64_t startQueryTime,int64_t endQueryTime,
		int64_t startBlockTime,int64_t endBlockTime,int blockItemNum,
		ORDER_TYPE order){

	double *sample = NULL;
	if(pField->type == FIELD_TYPE_DOUBLE && startBlockTime > pField->typeChangeTimestamp){
		ChunkIter_t *iter = Compressed_NewChunkIterator(&pCache->chunk);

		//sample = RedisModule_Alloc(sizeof(double) * blockItemNum);
		sample = RedisModule_Alloc(sizeof(double) * pCache->chunk.count);
		for(int i = 0; i < pCache->chunk.count; i++){
			Compressed_ChunkIteratorGetNext(iter, sample + i);
		}
		Compressed_FreeChunkIterator(iter);
	}

	int st = 0, end = blockItemNum,step = 1, sample_id = 0;
	if(order == ORDER_DESC){
		sample_id = pCache->chunk.count - 1;
		st = blockItemNum - 1;
		end = -1;
		step = -1;
	}

	int ret = 0;
	while(st != end){
		int64_t t = startBlockTime + st;
		if(t >= startQueryTime && t <= endQueryTime){
	    	if(!(pCache->haveTimestamp[st/8] & true_bits[st%8])){
	    		st += step;
	    		continue;
	    	}

	    	RedisModule_ReplyWithArray(ctx,2);
	    	RedisModule_ReplyWithLongLong(ctx,t);
			if(pCache->isValueNone[st/8] & true_bits[st%8]){
				//sprintf(out,"%" PRIu64 ",none\n",t);
				RedisModule_ReplyWithStringBuffer(ctx,"none",4);
			}else if(pField->type == FIELD_TYPE_BOOL || t < pField->typeChangeTimestamp){
				if(pCache->bvalue[st/8] & true_bits[st%8]){
					//sprintf(out,"%" PRIu64 ",1\n",t);
					RedisModule_ReplyWithBool(ctx,1);
				}else{
					//sprintf(out,"%" PRIu64 ",0\n",t);
					RedisModule_ReplyWithBool(ctx,0);
				}
			}else if(pField->type == FIELD_TYPE_DOUBLE){
				//sprintf(out,"%" PRIu64 ",%f\n",t,sample[st]);
				RedisModule_ReplyWithDouble(ctx,sample[sample_id]);
				sample_id += step;
			}
			ret++;
		}
		st += step;
	}

	if(sample) RedisModule_Free(sample);
	return ret;
}

int query_cache(RedisModuleCtx *ctx,FIELD *pField,int64_t startQueryTime,int64_t endQueryTime,ORDER_TYPE order){
	CACHE* pCache = pField->cache[gdb->cacheId];
	int64_t startBlockTime = gdb->startTimestamp;
	int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
	int blockItemNum = gdb->curRowNum;
	return query_block(ctx,pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order);
}

int query_file(RedisModuleCtx *ctx,FIELD *pField,int64_t startQueryTime,int64_t endQueryTime,ORDER_TYPE order){
	RedisModule_AutoMemory(ctx);
	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(ctx,"index:%s", pField->name);
	RedisModuleStreamID id;
	id.ms = startQueryTime*1000;
	id.seq = 0;
	RedisModuleString *start_stream_id = RedisModule_CreateStringFromStreamID(ctx, &id);
	id.ms = endQueryTime*1000;
	id.seq = 0;
	RedisModuleString *end_stream_id = RedisModule_CreateStringFromStreamID(ctx, &id);

	RedisModuleCallReply *reply = RedisModule_Call(rts_staticCtx, "XRANGE", "sss", stream_name ,start_stream_id,end_stream_id);
	size_t len = RedisModule_CallReplyLength(reply);
	char dbPathName[256];
	int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
	char* output = RedisModule_Alloc(output_size+1);
	int64_t tm = 0;
	int dataFd = 0;
	int ret = 0;
	for (int i = 0; i < len; i++) {
		RedisModuleCallReply* subReply = RedisModule_CallReplyArrayElement(reply,i);

		RedisModuleStreamID id;
		const RedisModuleString *str = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(subReply, 0));
		RedisModule_StringToStreamID(str, &id);
		if(tm != id.ms/3600000){
			tm = id.ms/3600000;
			sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->path,tm);
			if(dataFd)
				close(dataFd);
			dataFd = open(dbPathName, O_RDWR);
		}

		RedisModuleCallReply *fieldsReply = RedisModule_CallReplyArrayElement(subReply, 1);
		char* str_reply = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(fieldsReply, 1),0);
		long long offset = strtoll(str_reply,0,10);
		str_reply = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(fieldsReply, 3),0);
		long long length = strtoll(str_reply,0,10);

		lseek(dataFd, offset,SEEK_SET);
		char*input = RedisModule_Alloc(length);
		read(dataFd, input, length);

		lzf_decompress(input, length, output,output_size);
		RedisModule_Free(input);

		CACHE* pCache = (CACHE*)output;
		int64_t startBlockTime = id.ms/1000;
		int64_t endBlockTime = id.ms/1000 + CACHE_ROW_NUM - 1;
		int blockItemNum = CACHE_ROW_NUM;
		ret += query_block(ctx,pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order);
	}
	RedisModule_Free(output);
	if(dataFd)
		close(dataFd);
	RedisModule_FreeCallReply(reply);

	RedisModule_FreeString(ctx,stream_name);
	RedisModule_FreeString(ctx,start_stream_id);
	RedisModule_FreeString(ctx,end_stream_id);

	return ret;
}

int query(RedisModuleCtx *ctx,const char* device,const char* field,const char* strStartTime, const char* strEndTime,const char*  strOrder){//2018-06-01 08:00:00.000
	int64_t startQueryTime, endQueryTime;

	if (taosParseTime(strStartTime, &startQueryTime, strlen(strStartTime), TSDB_TIME_PRECISION_MILLI) != 0) {
		RedisModule_ReplyWithError(ctx,"ERR startQueryTime");
		return -1;
	}
	startQueryTime /= 1000;

	if(strcmp(strEndTime,"NOW") == 0 || strcmp(strEndTime,"now") == 0){
		endQueryTime = RedisModule_Milliseconds();
	}else{
		if (taosParseTime(strEndTime, &endQueryTime, strlen(strEndTime), TSDB_TIME_PRECISION_MILLI) != 0) {
			RedisModule_ReplyWithError(ctx,"ERR strEndTime");
			return -1;
		}
	}
	endQueryTime /= 1000;
	if(startQueryTime >=  endQueryTime){
		RedisModule_ReplyWithError(ctx,"ERR startQueryTime >=  endQueryTime");
		return -1;
	}

	ORDER_TYPE order;
	if(strcmp(strOrder,"ASC") == 0 || strcmp(strOrder,"asc") == 0)
		order = ORDER_ASC;
	else if(strcmp(strOrder,"DESC") == 0 || strcmp(strOrder,"desc") == 0)
		order = ORDER_DESC;
	else{
		RedisModule_ReplyWithError(ctx,"ERR strOrder");
		return -1;
	}

	char strFieldName[128];
	sprintf(strFieldName,"tag:%s:%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		RedisModule_ReplyWithError(ctx,"ERR field not exist!");
		return -1;
	}

	int total = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
	if(order == ORDER_DESC){
		total += query_cache(ctx,pField,startQueryTime,endQueryTime,order);
	}

	total += query_file(ctx,pField,startQueryTime,endQueryTime,order);

	if(order == ORDER_ASC){
		total += query_cache(ctx,pField,startQueryTime,endQueryTime,order);
	}
	RedisModule_ReplySetArrayLength(ctx, total);
	return 0;
}

