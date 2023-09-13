
/*
 * module.c
 *
 *  Created on: 2023Äê5ÔÂ24ÈÕ
 *      Author: a
 */
#define REDISMODULE_MAIN
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include "module.h"
#include "version.h"
#include "tsdb.h"
#include "rdb.h"
#include "fast_double_parser_c.h"
#include "ttime.h"
#include "node.h"
#include "rule.h"
#include "indexer.h"
#include "util.h"

RedisModuleType *DeviceType = NULL;
RedisModuleCtx *rts_staticCtx; // global redis ctx
unsigned int retentionHours = 3;
unsigned int retentionMbs = 1024;
RedisModuleString* edb_path = NULL;
int64_t lastSampleTimestamp;
int loading_ended = 0;

int lmgw_sub_sample(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc < 2)
		return RedisModule_WrongArity(ctx);

	const char *buf = RedisModule_StringPtrLen(argv[1], NULL);
	RedisModuleString *node_name = RedisModule_CreateStringPrintf(ctx,"node:%s", buf);

	RedisModuleCallReply *reply1;
	reply1 = RedisModule_Call(ctx, "HSCAN", "sccccc", node_name, "0", "MATCH","sub:*", "COUNT", "100");
	RedisModule_Assert(RedisModule_CallReplyType(reply1) == REDISMODULE_REPLY_ARRAY);
	RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply1,1);
	int type = RedisModule_CallReplyType(subReply);
	if (type == REDISMODULE_REPLY_NULL) {
		return RedisModule_WrongArity(ctx);
	}
	size_t len = RedisModule_CallReplyLength(subReply);
	if (len == 0) {
		return RedisModule_WrongArity(ctx);
	}
	RedisModule_Assert(len % 2 == 0);

	long devicelen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	for (int i = 0; i < len; i+=2) {
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(subReply, i);
		size_t key_len;
		const char* cstr_key = RedisModule_CallReplyStringPtr(keyReply,&key_len);
		RedisModuleString* device_name = RedisModule_CreateString(ctx,cstr_key+4,key_len-4);

		RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);
		int keytype = RedisModule_KeyType(device_key);
		if (keytype != REDISMODULE_KEYTYPE_MODULE) {
			RedisModule_CloseKey(device_key);
			RedisModule_FreeString(ctx,device_name);
			continue;
		}
		if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
			RedisModule_CloseKey(device_key);
			RedisModule_FreeString(ctx,device_name);
			continue;
		}

		Device* pDevice;
		if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
			RedisModule_CloseKey(device_key);
			RedisModule_FreeString(ctx,device_name);
			continue;
		}else{
			pDevice = RedisModule_ModuleTypeGetValue(device_key);
		}

		QFIELD* pFieldHead = NULL;
		QFIELD* pFieldPre = NULL;
	    dictIterator *di = dictGetSafeIterator(pDevice->fields);
	    dictEntry *de;
	    while ((de = dictNext(di)) != NULL) {
	    	FIELD *pField = dictGetVal(de);
			QFIELD* pFieldTemp = (QFIELD*)RedisModule_Calloc(1,sizeof(QFIELD));
			pFieldTemp->pField = pField;
			pFieldTemp->curCache = NULL;
			if(pFieldHead == NULL){
				pFieldHead = pFieldTemp;
			}else{
				pFieldPre->next = pFieldTemp;
			}
			pFieldPre = pFieldTemp;
	    }
	    dictReleaseIterator(di);

	    int64_t startQueryTime, endQueryTime;
	    RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(subReply, i+1);
	    size_t val_len;
	    const char* cstr_val = RedisModule_CallReplyStringPtr(valReply,&val_len);
	    startQueryTime = strtoll(cstr_val,NULL,10) + 1;
	    endQueryTime = RedisModule_Milliseconds();

	    RedisModule_ReplyWithArray(ctx, 2);
	    RedisModule_ReplyWithString(ctx, pDevice->keyName);
	    if(query_sample(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,ORDER_ASC,QUERY_SUB)){
	    	RedisModule_Call(ctx, "HSET", "!sbl", node_name, cstr_key,key_len,lastSampleTimestamp);
	    }
	    devicelen++;

	    RedisModule_CloseKey(device_key);
		RedisModule_FreeString(ctx,device_name);
	}
	RedisModule_ReplySetArrayLength(ctx, devicelen);

	RedisModule_FreeString(ctx,node_name);
	RedisModule_FreeCallReply(reply1);

	return REDISMODULE_OK;
}

RedisModuleString* lua_process(RedisModuleCtx *ctx,FIELD *pField,RedisModuleString *field_value) {

	RedisModuleCallReply *reply;

	if(pField->args_len == 0)
		reply = RedisModule_Call(ctx, "FCALL_RO", "scss",pField->func, "0", field_value);
	else if(pField->args_len == 1)
		reply = RedisModule_Call(ctx, "FCALL_RO", "scss",pField->func, "0", field_value, pField->args[0]);
	else if(pField->args_len == 2)
		reply = RedisModule_Call(ctx, "FCALL_RO", "scsss",pField->func, "0", field_value, pField->args[0], pField->args[1]);
	else if(pField->args_len == 3)
		reply = RedisModule_Call(ctx, "FCALL_RO", "scssss",pField->func, "0", field_value, pField->args[0], pField->args[1], pField->args[2]);
	else
		reply = RedisModule_Call(ctx, "FCALL_RO", "scsssss",pField->func, "0", field_value, pField->args[0], pField->args[1], pField->args[2], pField->args[3]);

	int type = RedisModule_CallReplyType(reply);
	if (type == REDISMODULE_REPLY_ERROR || type == REDISMODULE_REPLY_NULL) {
		return field_value;
	}

	RedisModuleString* ret = RedisModule_CreateStringFromCallReply(reply);
	RedisModule_FreeCallReply(reply);

	return ret;
}

int lmgw_pub_sample(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc % 2 == 0 || argc < 5)
		return RedisModule_WrongArity(ctx);


	const char* cstr_timestamp = RedisModule_StringPtrLen(argv[1],NULL);
	errno = 0;
	timestamp_t timestamp = strtoll(cstr_timestamp,NULL,10);
	if(errno != 0){
		RedisModule_ReplyWithError(ctx, "lmgw: the format of timestamp is not correct");
		return REDISMODULE_OK;
	}

	RedisModuleString *device_name = argv[2];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE && keytype != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_KeyType(device_key) == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_RetainString(ctx, device_name);
		pDevice = NewDevice(device_name);
		RedisModule_ModuleTypeSetValue(device_key, DeviceType, pDevice);
	}else if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device key");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}
	ResetDevice(pDevice);

	size_t row_id = pDevice->totalSamples;
	size_t numfields = (argc - 3) / 2;
	for (size_t i = 0; i < numfields; i++) {
		RedisModuleString* field_name = argv[3 + 2 * i];
		RedisModuleString* field_value = argv[3 + 2 * i + 1];
		const char* cstr_field_name = RedisModule_StringPtrLen(field_name,NULL);
		const char* cstr_field_value = RedisModule_StringPtrLen(field_value,NULL);

		FIELD *pField = dictFetchValue(pDevice->fields, cstr_field_name);
		if(pField == NULL){
			int cache_size = sizeof(double) * CACHE_ROW_NUM + 2*CACHE_BITMAP_SIZE;
			FIELD_TYPE ftype = FIELD_TYPE_DOUBLE;
			if(strcmp(cstr_field_value,"1") == 0 || strcmp(cstr_field_value,"0") == 0){
				ftype = FIELD_TYPE_BOOL;
				cache_size = 3*CACHE_BITMAP_SIZE;
			}
			pField = NewField(field_name,ftype,0);
			dictAdd(pDevice->fields, (void*)cstr_field_name, pField);

			pField->cache = RedisModule_Calloc(1,cache_size);
			RedisModule_RetainString(ctx, field_name);
		}
		if(pField->func){
			field_value = lua_process(ctx, pField,field_value);
		}
		cstr_field_value = RedisModule_StringPtrLen(field_value,NULL);
		add_sample(ctx,row_id,pField,cstr_field_value);
		if(pField->rules){
			double value;
			if(pField->type == FIELD_TYPE_DOUBLE){
				value = pField->cache->dvalue[row_id];
			}else if(bitmapTestBit(pField->cache->bvalue,row_id)){
				value = 1;
			}else{
				value = 0;
			}

			CompactionRule *rule = pField->rules;
			while (rule != NULL) {
				handleCompaction(ctx,rule,timestamp,value);
				rule = rule->nextRule;
			}
		}

		if(field_value != argv[3 + 2 * i + 1]){
			RedisModule_FreeString(ctx,field_value);
		}
	}
	RedisModule_ReplyWithSimpleString(ctx, "OK");

	pDevice->timestamps[pDevice->totalSamples] = timestamp;
	pDevice->totalSamples++;
	if(pDevice->totalSamples == CACHE_ROW_NUM){
		SaveDevice(ctx,pDevice);
		pDevice->totalSamples = 0;
	}

	RedisModule_ReplicateVerbatim(ctx);
	RedisModule_CloseKey(device_key);

	return REDISMODULE_OK;
}


int lmgw_sub_message(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc != 2)
		return RedisModule_WrongArity(ctx);

	size_t len;
	const char *buf = RedisModule_StringPtrLen(argv[1], &len);
	RedisModuleString *node_name = RedisModule_CreateStringPrintf(ctx,
			"node:%s", buf);

	RedisModuleKey *node_key;
	node_key = RedisModule_OpenKey(ctx, node_name, REDISMODULE_READ);
	if (RedisModule_KeyType(node_key) != REDISMODULE_KEYTYPE_HASH) {
		RedisModule_CloseKey(node_key);
		return RedisModule_WrongArity(ctx);
	}

	RedisModuleString *all_sid, *self_sid;
	RedisModule_HashGet(node_key, REDISMODULE_HASH_CFIELDS, "message:all", &all_sid,
			"message:self", &self_sid, NULL);
	if(all_sid == NULL){
		all_sid = RedisModule_CreateString(ctx,"0-0",3);
	}
	if(self_sid == NULL){
		self_sid = RedisModule_CreateString(ctx,"0-0",3);
	}

	RedisModuleString *message_self = RedisModule_CreateStringPrintf(ctx,
				"message:%s", buf);

	RedisModuleCallReply *reply2;
	reply2 = RedisModule_Call(ctx, "XREAD", "ccccccsss","COUNT","10","BLOCK","1000", "STREAMS", "message:all",message_self,all_sid,self_sid);
	RedisModule_ReplyWithCallReply(ctx, reply2);

	size_t xread_len = RedisModule_CallReplyLength(reply2);
	RedisModuleString* field_vals[xread_len];
	const char* msg_key = "message:all";
	size_t msg_key_len = strlen(msg_key);
	for (int i = 0; i < xread_len; i++) {
		RedisModuleCallReply* subReply = RedisModule_CallReplyArrayElement(reply2,i);
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(subReply, 0);
		const char *ptr = RedisModule_CallReplyStringPtr(keyReply,&len);
		if(strncmp(ptr,msg_key,msg_key_len) == 0)
			field_vals[i] = RedisModule_CreateStringFromCallReply(keyReply);
		else
			field_vals[i] = RedisModule_CreateString(ctx,"message:self",12);

		RedisModuleCallReply *valsReply = RedisModule_CallReplyArrayElement(subReply, 1);
		size_t vals_len = RedisModule_CallReplyLength(valsReply);
		RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(valsReply, vals_len - 1);

		field_vals[i+1] = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(valReply, 0));
	}
	RedisModule_Call(ctx, "HMSET", "!sv", node_name, field_vals,xread_len*2);

	return REDISMODULE_OK;
}

int lmgw_pub_message_call(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc % 2 != 0 || argc < 4)
		return RedisModule_WrongArity(ctx);

	size_t len;
	const char *target = RedisModule_StringPtrLen(argv[1], &len);
	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(ctx,
			"message:%s", target);

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "XADD", "!scclcv", stream_name,"MAXLEN", "~",1024,"*" ,argv+2, argc - 2);

	RedisModule_ReplyWithCallReply(ctx, reply);

	return REDISMODULE_OK;
}

int lmgw_query(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 5)
		return RedisModule_WrongArity(ctx);

	const char *strStartTime = RedisModule_StringPtrLen(argv[3], NULL);
	const char *strEndTime = RedisModule_StringPtrLen(argv[4], NULL);
	int64_t startQueryTime, endQueryTime;
	if (taosParseTime(strStartTime, &startQueryTime, strlen(strStartTime), TSDB_TIME_PRECISION_MILLI) != 0) {
		RedisModule_ReplyWithError(ctx,"lmgw: startQueryTime format is not correct,for example 2018-06-01 08:00:00.000");
		return REDISMODULE_ERR;
	}

	if(strcmp(strEndTime,"NOW") == 0 || strcmp(strEndTime,"now") == 0){
		endQueryTime = RedisModule_Milliseconds();
	}else{
		if (taosParseTime(strEndTime, &endQueryTime, strlen(strEndTime), TSDB_TIME_PRECISION_MILLI) != 0) {
			RedisModule_ReplyWithError(ctx,"lmgw: endQueryTime format is not correct,for example 2018-06-01 08:00:00.000");
			return REDISMODULE_OK;
		}
	}

	if(startQueryTime >= endQueryTime){
		RedisModule_ReplyWithError(ctx,"lmgw: startQueryTime >= endQueryTime");
		return REDISMODULE_OK;
	}

	ORDER_TYPE order = ORDER_ASC;
	if(argc > 5){
		const char *strOrder = RedisModule_StringPtrLen(argv[5], NULL);
		if(strcmp(strOrder,"desc") == 0 || strcmp(strOrder,"DESC") == 0)
			order = ORDER_DESC;
	}

	RedisModuleString *device_name = argv[1];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the key does not exist");
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device key");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	QFIELD* pFieldHead = NULL;
	QFIELD* pFieldPre = NULL;
	const char* names = RedisModule_StringPtrLen(argv[2], NULL);
	while(1){
		char* field_name;
		const char *next = strchr(names,',');
		if(next){
			field_name = RedisModule_Alloc(next - names + 1);
			strncpy(field_name,names,next - names);
		}else{
			field_name = names;
		}

		FIELD *pField = dictFetchValue(pDevice->fields, field_name);
		if(pField == NULL){
			char msg[128];
			snprintf(msg,127,"lmgw: the field[%s] does not exist",field_name);
			RedisModule_ReplyWithError(ctx, msg);
			goto err;
		}

		QFIELD* pFieldTemp = (QFIELD*)RedisModule_Calloc(1,sizeof(QFIELD));
		pFieldTemp->pField = pField;
		pFieldTemp->curCache = NULL;
		if(pFieldHead == NULL){
			pFieldHead = pFieldTemp;
		}else{
			pFieldPre->next = pFieldTemp;
		}
		pFieldPre = pFieldTemp;

		if(next){
			RedisModule_Free(field_name);
			names = next + 1;
		}else{
			break;
		}
	}

	query_sample(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,order,QUERY_NORMAL);

err:
	while(pFieldHead){
		QFIELD* curField = pFieldHead;
		pFieldHead = pFieldHead->next;

		if(curField->curCache)
			RedisModule_Free(curField->curCache);
		RedisModule_Free(curField);
	}
	RedisModule_CloseKey(device_key);
	return REDISMODULE_OK;
}

int lmgw_field_function(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 4)
		return RedisModule_WrongArity(ctx);

	RedisModuleString *device_name = argv[1];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the key does not exist");
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device key");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	const char* field_name = RedisModule_StringPtrLen(argv[2], NULL);
	FIELD *pField = dictFetchValue(pDevice->fields, field_name);
	if(pField == NULL){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a field key");
		return REDISMODULE_OK;
	}

	if(pField->func)
		RedisModule_FreeString(rts_staticCtx,pField->func);

	pField->func = argv[3];
	RedisModule_RetainString(ctx, argv[3]);

    if(pField->args){
    	for(int i = 0; i < pField->args_len; i++){
    		RedisModule_FreeString(rts_staticCtx,pField->args[i]);
    	}
    	RedisModule_Free(pField->args);
    }

    pField->args_len = argc-4;
	pField->args = RedisModule_Alloc(sizeof(RedisModuleString *)*(argc-4));
	for(int i = 4; i < argc; i++){
		pField->args[i-4] = argv[i];
		RedisModule_RetainString(ctx, argv[i]);
	}

	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);
	RedisModule_CloseKey(device_key);

	return REDISMODULE_OK;
}

int lmgw_field_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 3)
		return RedisModule_WrongArity(ctx);

	RedisModuleString *device_name = argv[1];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the device does not exist");
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	const char* field_name = RedisModule_StringPtrLen(argv[2], NULL);
	FIELD *pField = dictFetchValue(pDevice->fields, field_name);
	if(pField == NULL){
		RedisModule_ReplyWithError(ctx,"lmgw: the field does not exist");
		return REDISMODULE_OK;
	}

	ReplyFieldInfo(ctx,pField);

	RedisModule_CloseKey(device_key);
	return REDISMODULE_OK;
}

int lmgw_field_flags(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 5)
		return RedisModule_WrongArity(ctx);

	RedisModuleString *device_name = argv[1];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the key does not exist");
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device key");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	const char* str_op = RedisModule_StringPtrLen(argv[2], NULL);
	const char* str_flag = RedisModule_StringPtrLen(argv[3], NULL);
	int op = StringLenOpTypeToEnum(str_op,strlen(str_op));
	uint64_t flag = StringLenFlagToConst(str_flag,strlen(str_flag));
	for(int i = 4; i < argc; i++){
		const char* field_name = RedisModule_StringPtrLen(argv[i], NULL);
		FIELD *pField = dictFetchValue(pDevice->fields, field_name);
		if(pField == NULL){
			continue;
		}
		if(op)
			pField->flags |= flag;
		else
			pField->flags &= ~flag;
	}

	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);
	RedisModule_CloseKey(device_key);

	return REDISMODULE_OK;
}

int lmgw_node(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 3)
		return RedisModule_WrongArity(ctx);

	ControlNode(ctx,argv[1],argv[2]);
	return REDISMODULE_OK;
}

int lmgw_query_agg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc < 7)
		return RedisModule_WrongArity(ctx);

	const char *strStartTime = RedisModule_StringPtrLen(argv[3], NULL);
	const char *strEndTime = RedisModule_StringPtrLen(argv[4], NULL);
	int64_t startQueryTime, endQueryTime;
	if (taosParseTime(strStartTime, &startQueryTime, strlen(strStartTime), TSDB_TIME_PRECISION_MILLI) != 0) {
		RedisModule_ReplyWithError(ctx,"lmgw: startQueryTime format is not correct,for example 2018-06-01 08:00:00.000");
		return REDISMODULE_ERR;
	}

	if(strcmp(strEndTime,"NOW") == 0 || strcmp(strEndTime,"now") == 0){
		endQueryTime = RedisModule_Milliseconds();
	}else{
		if (taosParseTime(strEndTime, &endQueryTime, strlen(strEndTime), TSDB_TIME_PRECISION_MILLI) != 0) {
			RedisModule_ReplyWithError(ctx,"lmgw: endQueryTime format is not correct,for example 2018-06-01 08:00:00.000");
			return REDISMODULE_OK;
		}
	}

	if(startQueryTime >= endQueryTime){
		RedisModule_ReplyWithError(ctx,"lmgw: startQueryTime >= endQueryTime");
		return REDISMODULE_OK;
	}

	ORDER_TYPE order = ORDER_ASC;

	long long bucketDuration;
	if(RedisModule_StringToLongLong(argv[6],&bucketDuration)  != REDISMODULE_OK){
		RedisModule_ReplyWithError(ctx,"lmgw: period format is not correct");
	}

	int agg_type_len = 0;
	TS_AGG_TYPES_T agg_types[TS_AGG_TYPES_MAX];
	const char* agg_names = RedisModule_StringPtrLen(argv[5], NULL);
	while(1){
		TS_AGG_TYPES_T t;
		char *next = strchr(agg_names,',');
		if(next){
			t = StringLenAggTypeToEnum(agg_names,next - agg_names);
		}else{
			t = StringLenAggTypeToEnum(agg_names, strlen(agg_names));
		}
		if(t == TS_AGG_INVALID)
			continue;
		if(agg_type_len >= TS_AGG_TYPES_MAX)
			break;
		agg_types[agg_type_len++] = t;

		if(next){
			agg_names = next + 1;
		}else{
			break;
		}
	}

	RedisModuleString *device_name = argv[1];
	RedisModuleKey *device_key = RedisModule_OpenKey(ctx, device_name,REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(device_key);
	if (keytype != REDISMODULE_KEYTYPE_MODULE) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx,"lmgw: the key does not exist");
		return REDISMODULE_OK;
	}

	Device* pDevice;
	if(RedisModule_ModuleTypeGetType(device_key) != DeviceType){
		RedisModule_ReplyWithError(ctx, "lmgw: the key is not a device key");
		return REDISMODULE_OK;
	}else{
		pDevice = RedisModule_ModuleTypeGetValue(device_key);
	}

	QFIELD* pFieldHead = NULL;
	QFIELD* pFieldPre = NULL;
	const char* names = RedisModule_StringPtrLen(argv[2], NULL);
	while(1){
		char* field_name;
		char *next = strchr(names,',');
		if(next){
			field_name = RedisModule_Alloc(next - names + 1);
			strncpy(field_name,names,next - names);
		}else{
			field_name = names;
		}

		FIELD *pField = dictFetchValue(pDevice->fields, field_name);
		if(pField == NULL){
			char msg[128];
			snprintf(msg,127,"lmgw: the field[%s] does not exist",field_name);
			RedisModule_ReplyWithError(ctx, msg);
			goto err;
		}

		QFIELD* pFieldTemp = (QFIELD*)RedisModule_Calloc(1,sizeof(QFIELD));
		pFieldTemp->pField = pField;
		for(int i = 0; i < agg_type_len; i++){
			//RedisModuleString *destKey = RedisModule_CreateStringPrintf(ctx,"%s:%s",RedisModule_StringPtrLen(pField->keyName,NULL), AggTypeEnumToString(agg_types[i]));
		    CompactionRule *rule = NewRule(NULL,agg_types[i], bucketDuration, 0);
		    //RedisModule_RetainString(ctx, destKey);
		    if (pFieldTemp->rules == NULL) {
		    	pFieldTemp->rules = rule;
		    } else {
		        CompactionRule *last = pFieldTemp->rules;
		        while (last->nextRule != NULL)
		            last = last->nextRule;
		        last->nextRule = rule;
		    }
		}

		if(pFieldHead == NULL){
			pFieldHead = pFieldTemp;
		}else{
			pFieldPre->next = pFieldTemp;
		}
		pFieldPre = pFieldTemp;

		if(next){
			RedisModule_Free(field_name);
			names = next + 1;
		}else{
			break;
		}
	}

	query_sample(ctx,pDevice,pFieldHead,startQueryTime,endQueryTime,order,QUERY_AGG);

err:
	while(pFieldHead){
		QFIELD* curField = pFieldHead;
		pFieldHead = pFieldHead->next;

		if(curField->curCache)
			RedisModule_Free(curField->curCache);

		CompactionRule *rule = curField->rules;
		while (rule != NULL) {
			CompactionRule *nextRule = rule->nextRule;
			FreeCompactionRule(rule);
			rule = nextRule;
		}
		RedisModule_Free(curField);
	}
	RedisModule_CloseKey(device_key);
	return REDISMODULE_OK;
}

int lmgw_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Device *pDevice;
    RedisModuleKey *key;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const int status =
        GetDevice(ctx, argv[1], &key, &pDevice, REDISMODULE_READ | REDISMODULE_WRITE, false, false);
    if (!status) {
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("LABELS", argv, argc) > 0) {
        RemoveIndexedMetric(keyName);
        // free current labels
        FreeLabels(pDevice->labels, pDevice->labelsCount);

        // set new newLabels
        pDevice->labels = cCtx.labels;
        pDevice->labelsCount = cCtx.labelsCount;
        IndexMetric(keyName, pDevice->labels, pDevice->labelsCount);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);

    //RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.alter", keyName);

    return REDISMODULE_OK;
}


/* Timer callback. */
void DeleteOldFileHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    RedisModule_CreateTimer(ctx,EDB_FILE_PERIOD,DeleteOldFileHandler,NULL);
    DeleteOldFile(ctx);
}

/* Timer callback. */
void CheckNodeHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    RedisModule_CreateTimer(ctx,CHECK_NODE_PERIOD_MS,CheckNodeHandler,NULL);
    CheckNode(ctx);
}

void LoadingEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    if (sub == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    	LoadCache(ctx);
    	LoadNode(ctx);
    	RedisModule_CreateTimer(ctx,EDB_FILE_PERIOD,DeleteOldFileHandler,NULL);
    	RedisModule_CreateTimer(ctx,CHECK_NODE_PERIOD_MS,CheckNodeHandler,NULL);
    	loading_ended = 1;
    }
}

void ShutdownEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data){
	WaitNode(ctx);
	if(edb_path)
		RedisModule_FreeString(ctx,edb_path);
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
	if(loading_ended == 0)
		return REDISMODULE_OK;
	NotifyNode(ctx,type,event,key);

    // Will be called in replicaof or on load rdb on load time
    if (strcasecmp(event, "loaded") == 0) {
    	IndexDeviceFromName(ctx, key);
        return REDISMODULE_OK;
    }

    return REDISMODULE_OK;
}

void FlushEventCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if ((!memcmp(&eid, &RedisModuleEvent_FlushDB, sizeof(eid))) &&
        subevent == REDISMODULE_SUBEVENT_FLUSHDB_END) {
        RemoveAllIndexedMetrics();
    }
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (RedisModule_Init(ctx, "lmgw", LMGW_MODULE_VERSION,REDISMODULE_APIVER_1) ==REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

    if (argc < 3) {
        RedisModule_Log(ctx, "warning", "Failed to parse lmgw configurations. aborting...");
        return REDISMODULE_ERR;
    }

    char *endptr = NULL;
    edb_path = argv[0];
    RedisModule_RetainString(ctx, edb_path);
	retentionHours = strtol(RedisModule_StringPtrLen(argv[1], NULL),&endptr,10);
	retentionMbs = strtol(RedisModule_StringPtrLen(argv[2], NULL),&endptr,10);
	const char* path = RedisModule_StringPtrLen(argv[0], NULL);

	RedisModule_Log(ctx, "notice", "EDB path: %s,retentionHours: %d,retentionMbs: %d",path, retentionHours,retentionMbs);

	RedisModule_Log(ctx, "notice", "lmgw version %d", LMGW_MODULE_VERSION);
	rts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    RedisModuleTypeMethods tm = { .version = REDISMODULE_TYPE_METHOD_VERSION,
                                  .rdb_load = device_rdb_load,
                                  .rdb_save = device_rdb_save,
                                  .aof_rewrite = DefaultAofRewrite,
                                  //.mem_usage = SeriesMemUsage,
                                  //.copy = CopySeries,
                                  .free = FreeDevice };

	DeviceType = RedisModule_CreateDataType(ctx, "LM_DEVICE", TS_LATEST_ENCVER, &tm);
	if (DeviceType == NULL)
		return REDISMODULE_ERR;
	IndexInit();

	if (RedisModule_CreateCommand(ctx, "lmgw.pub_sample", lmgw_pub_sample,"write deny-oom", 1, 0, 1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.sub_sample", lmgw_sub_sample, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.pub_message", lmgw_pub_message_call,"write deny-oom", 1, 0, 1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.sub_message", lmgw_sub_message, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.query", lmgw_query, "readonly",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "lmgw.query_agg", lmgw_query_agg, "readonly",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.field_flags", lmgw_field_flags, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "lmgw.field_function", lmgw_field_function, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "lmgw.field_create_rule", lmgw_field_create_rule, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "lmgw.field_delete_rule", lmgw_field_delete_rule, "write",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "lmgw.field_info", lmgw_field_info, "readonly",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.node", lmgw_node, "readonly",0, 0, -1) ==REDISMODULE_ERR)
		return REDISMODULE_ERR;
	RMUtil_RegisterWriteDenyOOMCmd(ctx, "lmgw.alter", lmgw_alter);

	RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, FlushEventCallback);
	RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, ShutdownEventCallback);
	RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, LoadingEventCallback);
	RedisModule_SubscribeToKeyspaceEvents(ctx,REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_LOADED,NotifyCallback);


	return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx){
	return REDISMODULE_OK;
}
