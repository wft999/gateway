/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "rdb.h"

#include "endianconv.h"
#include "load_io_error_macros.h"

#include <inttypes.h>
#include <string.h>

extern dictType fieldsDictType;
CompactionRule *NewRule(RedisModuleString *destKey,int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment);

void DefaultAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DUMP", "s", key);
  if (rep != NULL && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_STRING) {
    size_t n;
    const char *s = RedisModule_CallReplyStringPtr(rep, &n);
    RedisModule_EmitAOF(aof, "RESTORE", "slb", key, 0, s, n);
  } else {
    RedisModule_Log(RedisModule_GetContextFromIO(aof), "warning", "Failed to emit AOF");
  }
  if (rep != NULL) {
    RedisModule_FreeCallReply(rep);
  }
  RedisModule_FreeThreadSafeContext(ctx);
}

void *device_rdb_load(RedisModuleIO *io, int encver) {
    if (encver < TS_ENC_VER || encver > TS_LATEST_ENCVER) {
        RedisModule_LogIOError(io, "error", "data is not in the correct encoding");
        return NULL;
    }

    Device* pDevice = (Device*)RedisModule_Alloc(sizeof(Device));
    //pDevice->timestampIndex = RedisModule_CreateDict(NULL);
    pDevice->keyName = NULL;
    pDevice->keyName = LoadString_IOError(io, goto err);
    pDevice->totalSamples = LoadUnsigned_IOError(io, goto err);
    RedisModule_Assert(pDevice->totalSamples <= CACHE_ROW_NUM);
	for (size_t sampleIndex = 0; sampleIndex < pDevice->totalSamples; sampleIndex++) {
		pDevice->timestamps[sampleIndex] = LoadUnsigned_IOError(io, goto err);
	}
/*	uint64_t numIndex = LoadUnsigned_IOError(io, goto err);
	for (int i = 0; i < numIndex; ++i) {
		size_t len;
    	char* key = RedisModule_LoadStringBuffer(io,&len);
    	RedisModule_Assert(len == sizeof(timestamp_t));
    	timestamp_t ts = *((timestamp_t*)key);
    	timestamp_t rax_key = ntohu64(ts);
    	char* data = RedisModule_LoadStringBuffer(io,&len);
    	RedisModule_Assert(len == sizeof(INDEX));
		dictOperator(pDevice->timestampIndex, data, rax_key, DICT_OP_SET);
	}*/

	pDevice->fields = dictCreate(&fieldsDictType);
	uint64_t fieldsCount = LoadUnsigned_IOError(io, goto err);
    for (int i = 0; i < fieldsCount; i++) {
    	RedisModuleString *fieldName = LoadString_IOError(io, goto err);
    	FIELD_TYPE fieldType = LoadUnsigned_IOError(io, goto err);
    	uint64_t fieldFlags = LoadUnsigned_IOError(io, goto err);
    	FIELD* pField = NewField(fieldName,fieldType,fieldFlags);
    	const char* cstr_keyName = RedisModule_StringPtrLen(pField->keyName, NULL);
    	int retval = dictAdd(pDevice->fields, (void*)cstr_keyName, pField);
    	if(retval != 0){
    		RedisModule_FreeString(NULL,pField->keyName);
    		RedisModule_Free(pField);
    	}

    	unsigned int exist = LoadUnsigned_IOError(io, goto err);
    	if(exist){
    		pField->func = LoadString_IOError(io, goto err);

    		unsigned int args_len = LoadUnsigned_IOError(io, goto err);
    		if(args_len){
    			pField->args = RedisModule_Alloc(sizeof(RedisModuleString *)*args_len);
    			for(int i = 0; i < args_len; i++)
    				pField->args[i] = LoadString_IOError(io, goto err);
    		}
    	}

    	CompactionRule *lastRule = NULL;
    	uint64_t rulesCount = LoadUnsigned_IOError(io, goto err);
    	for (int i = 0; i < rulesCount; i++) {
    		RedisModuleString *destKey = LoadString_IOError(io, goto err);;
			uint64_t bucketDuration = LoadUnsigned_IOError(io, goto err);
			uint64_t timestampAlignment = LoadUnsigned_IOError(io, goto err);
			uint64_t aggType = LoadUnsigned_IOError(io, goto err);
			timestamp_t startCurrentTimeBucket = LoadUnsigned_IOError(io, goto err);

			CompactionRule *rule = NewRule(destKey, aggType, bucketDuration, timestampAlignment);
			destKey = NULL;
			rule->startCurrentTimeBucket = startCurrentTimeBucket;

			if (i == 0) {
				pField->rules = rule;
			} else {
				lastRule->nextRule = rule;
			}
			if (rule->aggClass->readContext(rule->aggContext, io, encver)) {
				goto err;
			}
			lastRule = rule;
    	}

    	size_t string_buffer_size;
    	if(pField->type == FIELD_TYPE_BOOL){
    		pField->cache = (CACHE*)LoadStringBuffer_IOError(io,&string_buffer_size,goto err);
    		RedisModule_Assert(string_buffer_size == 3*CACHE_BITMAP_SIZE);
    	}else{
    		pField->cache = (CACHE*)LoadStringBuffer_IOError(io,&string_buffer_size,goto err);
    		RedisModule_Assert(string_buffer_size == 2*CACHE_BITMAP_SIZE + CACHE_ROW_NUM*getFieldTypeSize(pField));
		}

/*    	numIndex = LoadUnsigned_IOError(io, goto err);
    	for (int i = 0; i < numIndex; ++i) {
    		size_t len;
        	char* key = RedisModule_LoadStringBuffer(io,&len);
        	RedisModule_Assert(len == sizeof(timestamp_t));
        	timestamp_t ts = *((timestamp_t*)key);
        	timestamp_t rax_key = ntohu64(ts);
        	char* data = RedisModule_LoadStringBuffer(io,&len);
        	RedisModule_Assert(len == sizeof(INDEX));
    		dictOperator(pField->dataIndex, data, rax_key, DICT_OP_SET);
    	}*/
    }

    pDevice->labelsCount = LoadUnsigned_IOError(io, goto err);
    pDevice->labels = RedisModule_Calloc(pDevice->labelsCount, sizeof(Label));
    for (int i = 0; i < pDevice->labelsCount; i++) {
    	pDevice->labels[i].key = LoadString_IOError(io, goto err);
    	pDevice->labels[i].value = LoadString_IOError(io, goto err);
    }

    //IndexMetric(pDevice->keyName, pDevice->labels, pDevice->labelsCount);
    return pDevice;

err:
	if (pDevice->labels) {
		for (int i = 0; i < pDevice->labelsCount; i++) {
			if (pDevice->labels[i].key) {
				RedisModule_FreeString(NULL, pDevice->labels[i].key);
			}
			if (pDevice->labels[i].value) {
				RedisModule_FreeString(NULL, pDevice->labels[i].value);
			}
    	}
    	free(pDevice->labels);
	}

	FreeDevice(pDevice);
    return NULL;
}

unsigned int countRules(FIELD* pField) {
    unsigned int count = 0;
    CompactionRule *rule = pField->rules;
    while (rule != NULL) {
        count++;
        rule = rule->nextRule;
    }
    return count;
}

void device_rdb_save(RedisModuleIO *io, void *value) {
	Device *pDevice = value;
    RedisModule_SaveString(io, pDevice->keyName);
    RedisModule_SaveUnsigned(io, pDevice->totalSamples);
    for (size_t sampleIndex = 0; sampleIndex < pDevice->totalSamples; sampleIndex++) {
    	RedisModule_SaveUnsigned(io, pDevice->timestamps[sampleIndex]);
    }

/*    INDEX *index;
    uint64_t numIndex = RedisModule_DictSize(pDevice->timestampIndex);
    RedisModule_SaveUnsigned(io, numIndex);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(pDevice->timestampIndex, "^", NULL, 0);
    char* key;
    size_t keylen;
    while ((key = RedisModule_DictNextC(iter, &keylen, (void**)&index)) != NULL) {
    	RedisModule_SaveStringBuffer(io,key,keylen);
    	RedisModule_SaveStringBuffer(io,(const char*)index,sizeof(INDEX));
    }
    RedisModule_DictIteratorStop(iter);*/

    uint64_t fieldsCount = dictSize(pDevice->fields);
    RedisModule_SaveUnsigned(io, fieldsCount);

    dictIterator *di = dictGetSafeIterator(pDevice->fields);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
    	FIELD *pField = dictGetVal(de);
    	RedisModule_SaveString(io, pField->keyName);
    	RedisModule_SaveUnsigned(io, pField->type);
    	RedisModule_SaveUnsigned(io, pField->flags);

    	if(pField->func){
    		RedisModule_SaveUnsigned(io, 1);
    		RedisModule_SaveString(io, pField->func);

        	if(pField->args){
        		RedisModule_SaveUnsigned(io, pField->args_len);
        		for(int i = 0; i < pField->args_len; i++)
        			RedisModule_SaveString(io, pField->args[i]);
        	}else{
        		RedisModule_SaveUnsigned(io, 0);
        	}
    	}else{
    		RedisModule_SaveUnsigned(io, 0);
    	}

    	uint64_t rulesCount = countRules(pField);
    	RedisModule_SaveUnsigned(io, rulesCount);
    	CompactionRule *rule = pField->rules;
    	while (rule != NULL) {
    		CompactionRule *nextRule = rule->nextRule;
    		RedisModule_SaveString(io, rule->destKey);
    		RedisModule_SaveUnsigned(io, rule->bucketDuration);
    		RedisModule_SaveUnsigned(io, rule->timestampAlignment);
    		RedisModule_SaveUnsigned(io, rule->aggType);
    		RedisModule_SaveUnsigned(io, rule->startCurrentTimeBucket);
    		rule->aggClass->writeContext(rule->aggContext, io);
    		rule = nextRule;
    	}

    	if(pField->type == FIELD_TYPE_BOOL){
    		RedisModule_SaveStringBuffer(io,(const char*)pField->cache,3*CACHE_BITMAP_SIZE);
    		/*int len = (pDevice->totalSamples % 8) ? (pDevice->totalSamples / 8 + 1) : (pDevice->totalSamples / 8);
    		RedisModule_SaveStringBuffer(io,pField->cache->haveTimestamp,len);
    		RedisModule_SaveStringBuffer(io,pField->cache->isValueNone,len);
    		RedisModule_SaveStringBuffer(io,pField->cache->bvalue,len);*/
    	}else{
    		RedisModule_SaveStringBuffer(io,(const char*)pField->cache,2*CACHE_BITMAP_SIZE + getFieldTypeSize(pField)*CACHE_ROW_NUM);
    		/*for (size_t sampleIndex = 0; sampleIndex < pDevice->totalSamples; sampleIndex++) {
    			RedisModule_SaveDouble(io,pField->cache->dvalue[sampleIndex]);
    		}*/
    	}

/*        numIndex = RedisModule_DictSize(pField->dataIndex);
        RedisModule_SaveUnsigned(io, numIndex);
        iter = RedisModule_DictIteratorStartC(pField->dataIndex, "^", NULL, 0);
        while ((key = RedisModule_DictNextC(iter, &keylen, (void**)&index)) != NULL) {
        	RedisModule_SaveStringBuffer(io,(const char*)key,keylen);
        	RedisModule_SaveStringBuffer(io,(const char*)index,sizeof(INDEX));
        }
        RedisModule_DictIteratorStop(iter);*/
    }
    dictReleaseIterator(di);

    RedisModule_SaveUnsigned(io, pDevice->labelsCount);
    for (int i = 0; i < pDevice->labelsCount; i++) {
        RedisModule_SaveString(io, pDevice->labels[i].key);
        RedisModule_SaveString(io, pDevice->labels[i].value);
    }
}
