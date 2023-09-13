/*
 * device.c
 *
 *  Created on: 2023Äê9ÔÂ4ÈÕ
 *      Author: a
 */
#include <string.h>
#include <ctype.h>

#include "device.h"

#define UNUSED(V) ((void) V)

extern RedisModuleType *DeviceType;
extern RedisModuleCtx *rts_staticCtx;

const int fieldTypeSize[] = {0,sizeof(char),sizeof(short),sizeof(int),sizeof(int64_t),sizeof(float),sizeof(double),sizeof(int64_t),0};
const char* fieldTypeName[] = {"BOOL","TING_INT","SMALL_INT","INT","BIG_INT","FLOAT","DOUBLE","TIMESTAMP","Unknown"};

int StringLenOpTypeToEnum(const char *op_type, size_t len) {
    int result = FLAG_OP_INVALID;
    char op_type_lower[len];
    for (int i = 0; i < len; i++) {
        op_type_lower[i] = tolower(op_type[i]);
    }
    if (len == 3) {
        if (strncmp(op_type_lower, "set", len) == 0) {
            result = FLAG_OP_SET;
        }
    } else if (len == 5) {
        if (strncmp(op_type_lower, "reset", len) == 0) {
            result = FLAG_OP_RESET;
        }
    }
    return result;
}

uint64_t StringLenFlagToConst(const char *flag, size_t len) {
	uint64_t result = FIELD_FLAG_NONE;
    char flag_lower[len];
    for (int i = 0; i < len; i++) {
    	flag_lower[i] = tolower(flag[i]);
    }
    if (len == 10) {
        if (strncmp(flag_lower, "accumulate", len) == 0) {
            result = FIELD_FLAG_ACC;
        }
    } else if (len == 5) {

    }
    return result;
}

/* Test bit 'pos' in a generic bitmap. Return 1 if the bit is set,
 * otherwise 0. */
int bitmapTestBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    return (bitmap[byte] & (1<<bit)) != 0;
}

/* Set the bit at position 'pos' in a bitmap. */
void bitmapSetBit(unsigned char *bitmap, int pos) {
	RedisModule_Assert(pos < CACHE_ROW_NUM);
    off_t byte = pos/8;
    int bit = pos&7;
    bitmap[byte] |= 1<<bit;
}

/* Clear the bit at position 'pos' in a bitmap. */
void bitmapClearBit(unsigned char *bitmap, int pos) {
	RedisModule_Assert(pos < CACHE_ROW_NUM);
    off_t byte = pos/8;
    int bit = pos&7;
    bitmap[byte] &= ~(1<<bit);
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
    RedisModule_FreeString(rts_staticCtx,pField->keyName);
    if(pField->func)
    	RedisModule_FreeString(rts_staticCtx,pField->func);

    if(pField->args){
    	for(int i = 0; i < pField->args_len; i++){
    		RedisModule_FreeString(rts_staticCtx,pField->args[i]);
    	}
    	RedisModule_Free(pField->args);
    }

    CompactionRule *rule = pField->rules;
	while (rule != NULL) {
		CompactionRule *nextRule = rule->nextRule;
		FreeCompactionRule(rule);
		rule = nextRule;
	}

	//FreeIndex(pField->dataIndex);
	RedisModule_Free(pField->cache);
    RedisModule_Free(val);
}
dictType fieldsDictType = {
	distCStrHash,            	/* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
	distCStrKeyCompare,      	/* key compare */
	NULL,//dictFieldNameDestructor,    /* key destructor */
	dictFieldDestructor,        /* val destructor */
    NULL                        /* allow to expand */
};

FIELD* NewField(RedisModuleString *keyName,FIELD_TYPE type,uint64_t flags) {
/*	size_t cache_size = 0;
	switch(type){
	case FIELD_TYPE_BOOL:
		cache_size = CACHE_ROW_NUM/BITS_PER_BYTE;
		break;
	case FIELD_TYPE_DOUBLE:
		cache_size = CACHE_ROW_NUM*sizeof(double);
		break;
	}*/
	FIELD* pField = (FIELD*)RedisModule_Calloc(1,sizeof(FIELD));
	pField->keyName = keyName;
	pField->type = type;
	pField->flags = flags;
	pField->rules = NULL;
	pField->func = NULL;
	pField->args = NULL;
	pField->args_len = 0;
	//pField->dataIndex = RedisModule_CreateDict(NULL);

	return pField;
}

int fieldHaveValue(FIELD* pField,size_t i){
	if(bitmapTestBit(pField->cache->isValueNone,i))
		return 0;
	if(bitmapTestBit(pField->cache->haveTimestamp,i))
		return 1;
	return 0;
}

int getFieldTypeSize(FIELD* pField){
	return fieldTypeSize[pField->type];
}

Device* NewDevice(RedisModuleString *keyName) {
	Device* pDevice = (Device*)RedisModule_Alloc(sizeof(Device));
	pDevice->fields = dictCreate(&fieldsDictType);
	pDevice->keyName = keyName;
	pDevice->totalSamples = 0;
	pDevice->pDeviceIndex = RedisModule_CreateDict(NULL);

	return pDevice;
}

void FreeDevice(void *value) {
	Device *pDevice = (Device *)value;
	dictRelease(pDevice->fields);
	FreeIndex(pDevice->pDeviceIndex);

    if (pDevice->keyName) {
        RedisModule_FreeString(NULL, pDevice->keyName);
    }

    RedisModule_Free(value);
}

void ResetDevice(Device* pDevice){
	size_t last;
	if(pDevice->totalSamples > 0)
		last = pDevice->totalSamples - 1;
	else
		last = CACHE_ROW_NUM - 1;

    dictIterator *di = dictGetSafeIterator(pDevice->fields);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
    	FIELD *pField = dictGetVal(de);
    	bitmapClearBit(pField->cache->haveTimestamp,pDevice->totalSamples);
    	pField->cache->dvalue[pDevice->totalSamples] = pField->cache->dvalue[last];
    }
    dictReleaseIterator(di);
}

int GetDevice(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
			  Device **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent) {
    if (shouldDeleteRefs) {
        mode = mode | REDISMODULE_WRITE;
    }
    RedisModuleKey *new_key = RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(new_key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
        	RedisModule_ReplyWithError(ctx, "LMGW: the key does not exist");
        }
        return FALSE;
    }
    if (RedisModule_ModuleTypeGetType(new_key) != DeviceType) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
        	RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        return FALSE;
    }

    *series = RedisModule_ModuleTypeGetValue(new_key);
    *key = new_key;

/*    if (shouldDeleteRefs) {
        deleteReferenceToDeletedSeries(ctx, *series);
    }*/

    return TRUE;
}

void ReplyFieldInfo(RedisModuleCtx *ctx,FIELD* pField){
	RedisModule_ReplyWithMap(ctx, 4);

	RedisModule_ReplyWithSimpleString(ctx,"Type");
	RedisModule_ReplyWithSimpleString(ctx,fieldTypeName[pField->type]);

	long flag_len = 0;
	RedisModule_ReplyWithSimpleString(ctx,"Flags");
	RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_LEN);
	if(pField->flags & FIELD_FLAG_ACC){
		flag_len++;
		RedisModule_ReplyWithSimpleString(ctx,"accumulate");
	}
	RedisModule_ReplySetArrayLength(ctx,flag_len);

	long func_len = 0;
	RedisModule_ReplyWithSimpleString(ctx,"Lua function");
	RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_LEN);
	if(pField->func){
		func_len++;
		RedisModule_ReplyWithString(ctx,pField->func);
	}
	for(int i = 0; i < pField->args_len;i++){
		func_len++;
		RedisModule_ReplyWithString(ctx,pField->args[i]);
	}
	RedisModule_ReplySetArrayLength(ctx,func_len);

	long rule_len = 0;
	RedisModule_ReplyWithSimpleString(ctx,"Rules");
	RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_LEN);
	CompactionRule *last = pField->rules;
	while (last != NULL){
		rule_len++;
		RedisModule_ReplyWithMap(ctx,2);
		RedisModule_ReplyWithSimpleString(ctx,"AggType");
		RedisModule_ReplyWithSimpleString(ctx,AggTypeEnumToString(last->aggType));
		RedisModule_ReplyWithSimpleString(ctx,"Duration");
		RedisModule_ReplyWithLongLong(ctx,last->bucketDuration);
		last = last->nextRule;
	}
	RedisModule_ReplySetArrayLength(ctx,rule_len);
}

