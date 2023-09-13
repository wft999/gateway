/*
 * rule.c
 *
 *  Created on: 2023Äê9ÔÂ4ÈÕ
 *      Author: a
 */
#include "rule.h"
#include "node.h"
#include "device.h"
#include "tsdb.h"
#include "util.h"
#include "query_language.h"

#include <inttypes.h>
#include <string.h>
extern RedisModuleType *DeviceType;

void FreeCompactionRule(void *value) {
    CompactionRule *rule = (CompactionRule *)value;
    if(rule->destKey)
    	RedisModule_FreeString(rts_staticCtx,rule->destKey);
    ((AggregationClass *)rule->aggClass)->freeContext(rule->aggContext);
    RedisModule_Free(rule);
}

CompactionRule *NewRule(RedisModuleString *destKey,int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment) {
    if (bucketDuration == 0ULL) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)RedisModule_Alloc(sizeof(CompactionRule));
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

int addAggSample(RedisModuleCtx *ctx,CompactionRule *rule,double value){
	Device* pDevice = NULL;
	size_t len = strlen("aggregation");
	RedisModuleString *keyName = RedisModule_CreateString(ctx,"aggregation",len);
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, keyName,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE && keytype != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_Log(ctx, "warning", "addAggSample Failed");
		return -1;
	}

	if(RedisModule_KeyType(device_key) == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_RetainString(ctx, keyName);
		pDevice = NewDevice(keyName);
		RedisModule_ModuleTypeSetValue(device_key, DeviceType, pDevice);
	}else if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_Log(ctx, "warning", "addAggSample Failed");
		return -2;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	const char* cstr_field_name = RedisModule_StringPtrLen(rule->destKey,NULL);
	FIELD *pField = dictFetchValue(pDevice->fields, cstr_field_name);
	if(pField == NULL){
		int cache_size = CACHE_BITMAP_SIZE*2 + sizeof(double) * CACHE_ROW_NUM;
		FIELD_TYPE ftype = FIELD_TYPE_DOUBLE;

		pField = NewField(rule->destKey,ftype,0);
		dictAdd(pDevice->fields, (void*)cstr_field_name, pField);
		pField->cache = RedisModule_Calloc(1,cache_size);
		RedisModule_RetainString(ctx, rule->destKey);
	}

	size_t rowId = pDevice->totalSamples;
	pField->cache->dvalue[rowId] = value;
	bitmapSetBit(pField->cache->haveTimestamp,rowId);

	pDevice->timestamps[pDevice->totalSamples] = rule->startCurrentTimeBucket;;
	pDevice->totalSamples++;
	if(pDevice->totalSamples == CACHE_ROW_NUM){
		SaveDevice(ctx,pDevice);
		pDevice->totalSamples = 0;
	}

	RedisModule_CloseKey(device_key);

	return 0;
}

// Calculate the begining of aggregation bucket
static inline timestamp_t CalcBucketStart(timestamp_t ts,
                                          timestamp_t bucketDuration,
                                          timestamp_t timestampAlignment) {
    const int64_t timestamp_diff = ts - timestampAlignment;
    return ts - modulo(timestamp_diff, (int64_t)bucketDuration);
}
static inline timestamp_t BucketStartNormalize(timestamp_t bucketTS) {
    return max(0, (int64_t)bucketTS);
}

int handleQueryCompaction(CompactionRule *rule,timestamp_t timestamp,double value,double* aggVal) {
	int ret = 0;
    timestamp_t currentTimestamp = CalcBucketStart(timestamp, rule->bucketDuration, rule->timestampAlignment);
    timestamp_t currentTimestampNormalized = BucketStartNormalize(currentTimestamp);

    if (rule->startCurrentTimeBucket == -1LL) {
        // first sample, lets init the startCurrentTimeBucket
        rule->startCurrentTimeBucket = currentTimestampNormalized;

        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }
    }

    if (currentTimestampNormalized > rule->startCurrentTimeBucket) {
    	ret = 1;
        if (rule->aggClass->addNextBucketFirstSample) {
            rule->aggClass->addNextBucketFirstSample(rule->aggContext, value, timestamp);
        }

        rule->aggClass->finalize(rule->aggContext, aggVal);

        Sample last_sample;
        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->getLastSample(rule->aggContext, &last_sample);
        }
        rule->aggClass->resetContext(rule->aggContext);
        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }

        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->addPrevBucketLastSample(
                rule->aggContext, last_sample.value, last_sample.timestamp);
        }
        rule->startCurrentTimeBucket = currentTimestampNormalized;
    }

    rule->aggClass->appendValue(rule->aggContext, value, timestamp);
    return ret;
}

void handleCompaction(RedisModuleCtx *ctx,CompactionRule *rule,timestamp_t timestamp,double value) {
    timestamp_t currentTimestamp = CalcBucketStart(timestamp, rule->bucketDuration, rule->timestampAlignment);
    timestamp_t currentTimestampNormalized = BucketStartNormalize(currentTimestamp);

    if (rule->startCurrentTimeBucket == -1LL) {
        // first sample, lets init the startCurrentTimeBucket
        rule->startCurrentTimeBucket = currentTimestampNormalized;

        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }
    }

    if (currentTimestampNormalized > rule->startCurrentTimeBucket) {
        if (rule->aggClass->addNextBucketFirstSample) {
            rule->aggClass->addNextBucketFirstSample(rule->aggContext, value, timestamp);
        }

        double aggVal;
        rule->aggClass->finalize(rule->aggContext, &aggVal);
        addAggSample(ctx,rule, aggVal);
        //add_agg_sample(ctx,pDevice,rule->destKey, rule->startCurrentTimeBucket, aggVal);

        Sample last_sample;
        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->getLastSample(rule->aggContext, &last_sample);
        }
        rule->aggClass->resetContext(rule->aggContext);
        if (rule->aggClass->addBucketParams) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }

        if (rule->aggClass->addPrevBucketLastSample) {
            rule->aggClass->addPrevBucketLastSample(
                rule->aggContext, last_sample.value, last_sample.timestamp);
        }
        rule->startCurrentTimeBucket = currentTimestampNormalized;
    }

    rule->aggClass->appendValue(rule->aggContext, value, timestamp);
}

CompactionRule *AddRule(RedisModuleCtx *ctx,FIELD *pField,
							  RedisModuleString *destKey,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment) {
    CompactionRule *rule = NewRule(destKey, aggType, bucketDuration, timestampAlignment);
    if (rule == NULL) {
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

int lmgw_field_create_rule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 6 && argc != 7) {
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    api_timestamp_t bucketDuration;
    int aggType;
    timestamp_t alignmentTS;
    const int result = _parseAggregationArgs(ctx, argv, argc, &bucketDuration, &aggType, NULL,NULL, &alignmentTS);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *deviceKeyName = argv[1];
    RedisModuleString *fieldKeyName = argv[2];

	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, deviceKeyName,REDISMODULE_READ | REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_ERR;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the device does not exist");
		return REDISMODULE_ERR;
	}

	Device* pDevice = RedisModule_ModuleTypeGetValue(device_key);
	const char* cstr_field_name = RedisModule_StringPtrLen(fieldKeyName,NULL);
	FIELD *pField = dictFetchValue(pDevice->fields, cstr_field_name);
	if(pField == NULL){
		RedisModule_CloseKey(device_key);
		RedisModule_ReplyWithError(ctx,"lmgw: the field does not exist");
		return REDISMODULE_ERR;
	}

	CompactionRule *last = pField->rules;
	while (last != NULL){
		if(last->bucketDuration == bucketDuration && last->aggType == aggType){
			RedisModule_CloseKey(device_key);
			RedisModule_ReplyWithError(ctx,"lmgw: the rule has existed");
			return REDISMODULE_ERR;
		}
		last = last->nextRule;
	}

	const char* cstr_type = AggTypeEnumToString(aggType);
	const char* cstr_device_name = RedisModule_StringPtrLen(deviceKeyName,NULL);
	RedisModuleString *destKey = RedisModule_CreateStringPrintf(ctx,"%s:%s:%s%" PRIu64,cstr_device_name,cstr_field_name,cstr_type,bucketDuration);
    if (AddRule(ctx, pField,destKey,aggType, bucketDuration, alignmentTS) == NULL) {
        RedisModule_CloseKey(device_key);
        RedisModule_ReplyWithSimpleString(ctx, "lmgw: ERROR creating rule");
        return REDISMODULE_ERR;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(device_key);

    return REDISMODULE_OK;
}

int lmgw_field_delete_rule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    if (argc != 6 && argc != 7) {
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    api_timestamp_t bucketDuration;
    int aggType;
    timestamp_t alignmentTS;
    const int result = _parseAggregationArgs(ctx, argv, argc, &bucketDuration, &aggType, NULL,NULL, &alignmentTS);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *deviceKeyName = argv[1];
    RedisModuleString *fieldKeyName = argv[2];

	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, deviceKeyName,REDISMODULE_READ | REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_ERR;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the device does not exist");
		return REDISMODULE_ERR;
	}

	Device* pDevice = RedisModule_ModuleTypeGetValue(device_key);
	const char* cstr_field_name = RedisModule_StringPtrLen(fieldKeyName,NULL);
	FIELD *pField = dictFetchValue(pDevice->fields, cstr_field_name);
	if(pField == NULL){
		RedisModule_CloseKey(device_key);
		RedisModule_ReplyWithError(ctx,"lmgw: the field does not exist");
		return REDISMODULE_ERR;
	}

	CompactionRule *pre = NULL;
	CompactionRule *last = pField->rules;
	while (last != NULL){
		if(last->bucketDuration == bucketDuration && last->aggType == aggType){
			if(pre){
				pre->nextRule = last->nextRule;
			}else{
				pField->rules = last->nextRule;
			}
			FreeCompactionRule(last);

			RedisModule_CloseKey(device_key);
			RedisModule_ReplyWithSimpleString(ctx, "OK");
			RedisModule_ReplicateVerbatim(ctx);
			return REDISMODULE_OK;
		}
		pre = last;
		last = last->nextRule;
	}

    RedisModule_ReplyWithError(ctx,"lmgw: the rule not existed");
    RedisModule_CloseKey(device_key);

    return REDISMODULE_ERR;
}
