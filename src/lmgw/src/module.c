
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

#include "../../lmgw/inc/module.h"
#include "../../lmgw/inc/version.h"
#include "../../lmgw/inc/db.h"
#include "fast_double_parser_c.h"
RedisModuleCtx *rts_staticCtx; // global redis ctx

int lmgw_sub_sample(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc < 2)
		return RedisModule_WrongArity(ctx);

	size_t len;
	const char *buf = RedisModule_StringPtrLen(argv[1], &len);
	RedisModuleString *node_name = RedisModule_CreateStringPrintf(ctx,
			"node:%s", buf);

	RedisModuleCallReply *reply1;
	reply1 = RedisModule_Call(ctx, "HSCAN", "sccccc", node_name, "0", "MATCH",
			"sample:*", "COUNT", "100");
	RedisModule_Assert(
			RedisModule_CallReplyType(reply1) == REDISMODULE_REPLY_ARRAY);
	RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply1,
			1);
	int type = RedisModule_CallReplyType(subReply);
	if (type == REDISMODULE_REPLY_NULL) {
		return RedisModule_WrongArity(ctx);
	}
	len = RedisModule_CallReplyLength(subReply);
	if (len == 0) {
		return RedisModule_WrongArity(ctx);
	}
	RedisModule_Assert(len % 2 == 0);
	RedisModuleString* fields[len/2];
	RedisModuleString* vals[len/2];
	for (int i = 0; i < len; i+=2) {
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(subReply, i);
		fields[i/2] = RedisModule_CreateStringFromCallReply(keyReply);

		RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(subReply, i+1);
		vals[i/2] = RedisModule_CreateStringFromCallReply(valReply);
	}

	RedisModuleCallReply *reply2;
	reply2 = RedisModule_Call(ctx, "XREAD", "cccccvv","COUNT","10","BLOCK","1000","STREAMS", fields,len/2,vals,len/2);
	RedisModule_ReplyWithCallReply(ctx, reply2);

	size_t xread_len = RedisModule_CallReplyLength(reply2);
	RedisModuleString* field_vals[xread_len*2];
	for (int i = 0; i < xread_len; i++) {
		subReply = RedisModule_CallReplyArrayElement(reply2,i);
		RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(subReply, 0);
		field_vals[2*i] = RedisModule_CreateStringFromCallReply(keyReply);

		RedisModuleCallReply *valsReply = RedisModule_CallReplyArrayElement(subReply, 1);
		size_t vals_len = RedisModule_CallReplyLength(valsReply);
		RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(valsReply, vals_len - 1);

		field_vals[2*i+1] = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(valReply, 0));
	}
	RedisModule_Call(ctx, "HMSET", "!sv", node_name, field_vals,xread_len*2);

	return REDISMODULE_OK;
}


RedisModuleString* lua_process(RedisModuleCtx *ctx,
		RedisModuleString *func, RedisModuleString *args,
		RedisModuleString *field_value) {

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "FCALL_RO", "scss",
			func, "1", field_value, args);
	int type = RedisModule_CallReplyType(reply);
	if (type == REDISMODULE_REPLY_ERROR || type == REDISMODULE_REPLY_NULL) {
		return field_value;
	}

	RedisModuleString* ret = RedisModule_CreateStringFromCallReply(reply);
	RedisModule_FreeCallReply(reply);

	return ret;
}

static int handleCompaction(RedisModuleCtx *ctx,
                             CompactionRule *rule,
							 timestamp_t timestamp,
                             double value,
							 double* aggVal) {
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
        if (rule->aggClass->addNextBucketFirstSample) {
            rule->aggClass->addNextBucketFirstSample(rule->aggContext, value, timestamp);
        }

        //double aggVal;
        rule->aggClass->finalize(rule->aggContext, aggVal);
        //SeriesAddSample(destSeries, rule->startCurrentTimeBucket, aggVal);

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
        ret = 1;
    }
    rule->aggClass->appendValue(rule->aggContext, value, timestamp);

    return ret;
}

int lmgw_pub_sample(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc % 2 != 0 || argc < 4)
		return RedisModule_WrongArity(ctx);

	size_t len;
	const char *device = RedisModule_StringPtrLen(argv[1], &len);
	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(ctx,
			"sample:%s", device);
	RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, stream_name,
	REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(stream_key);
	if (keytype != REDISMODULE_KEYTYPE_STREAM
			&& keytype != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}
	size_t numfields = (argc - 2) / 2;
	RedisModuleString *keyvalues[argc - 2];
	for (size_t i = 0; i < numfields; i++) {
		RedisModuleString* device_name = argv[1];
		RedisModuleString* field_name = argv[2 + 2 * i];
		RedisModuleString* field_value = argv[2 + 2 * i + 1];

		keyvalues[2 * i] = field_name;
		keyvalues[2 * i + 1] = field_value;

		const char *device = RedisModule_StringPtrLen(device_name, &len);
		const char *field = RedisModule_StringPtrLen(field_name, &len);
		RedisModuleString *tag_name = RedisModule_CreateStringPrintf(ctx,
					"tag:%s:%s", device, field);

		RedisModuleKey *tag_key;
		tag_key = RedisModule_OpenKey(ctx, tag_name, REDISMODULE_READ);
		int type = RedisModule_KeyType(tag_key);
		if (type == REDISMODULE_KEYTYPE_HASH) {
			RedisModuleString *func, *args, *agg_names;
			RedisModule_HashGet(tag_key, REDISMODULE_HASH_CFIELDS, "function", &func,
								"arguments", &args,"aggregation",&agg_names, NULL);
			if(func){
				keyvalues[2 * i + 1] = lua_process(ctx, func, args,field_value);
			}
			if(agg_names){
				//agg_process(ctx,agg_names,device,field,keyvalues[2 * i + 1]);
			}
		}
		RedisModule_CloseKey(tag_key);

		const char* value = RedisModule_StringPtrLen(keyvalues[2 * i + 1],NULL);
		int rowId = get_row_id(RedisModule_Milliseconds() / 1000);
		//put_db(ctx,rowId,device,field,value);
	}

	RedisModuleStreamID id;
	if (RedisModule_StreamAdd(stream_key, REDISMODULE_STREAM_ADD_AUTOID, &id, keyvalues,
			numfields) == REDISMODULE_ERR) {
		switch (errno) {
		case ERANGE:
			RedisModule_ReplyWithError(ctx,
					"ERR the elements are too large to be stored");
			break;
		case EBADF:
			RedisModule_ReplyWithError(ctx,
					"ERR the key was not opened for writing");
			break;
		case ENOTSUP:
			RedisModule_ReplyWithError(ctx,
					"ERR the key refers to a value of a type other than stream");
			break;
		default:
			RedisModule_ReplyWithError(ctx,
					"ERR called with invalid arguments");
			break;
		}
	} else {
		RedisModuleString *str = RedisModule_CreateStringFromStreamID(ctx, &id);
		RedisModule_ReplyWithString(ctx, str);
	}

	return REDISMODULE_OK;
}

int lmgw_pub_sample_call(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc % 2 != 0 || argc < 4)
		return RedisModule_WrongArity(ctx);
	if(!db_ready()){
		RedisModule_ReplyWithError(ctx,"edb is not ready");
		return REDISMODULE_ERR;
	}

	long long timestamp = RedisModule_Milliseconds();
	int rowId = get_row_id( timestamp/ 1000);
	const char *device = RedisModule_StringPtrLen(argv[1], NULL);
	size_t numfields = (argc - 2) / 2;
	RedisModuleString *keyvalues[argc - 2];
	RedisModuleString *aggvalues[argc - 2];
	size_t num_agg_fields = 0;
	for (size_t i = 0; i < numfields; i++) {
		RedisModuleString* field_name = argv[2 + 2 * i];
		RedisModuleString* field_value = argv[2 + 2 * i + 1];

		keyvalues[2 * i] = field_name;
		keyvalues[2 * i + 1] = field_value;

		const char *field = RedisModule_StringPtrLen(field_name, NULL);
		FIELD *pField = get_field(ctx,device,field);
		if(pField == NULL)
			continue;

		if(pField->func){
			keyvalues[2 * i + 1] = lua_process(ctx, pField->func, pField->args,field_value);
		}

		const char* value = RedisModule_StringPtrLen(keyvalues[2 * i + 1],NULL);
		if(pField->rules){
			double sample;
			double aggVal;
			fast_double_parser_c_parse_number(value, &sample);
			CompactionRule *rule = pField->rules;
			while (rule != NULL) {
				if(num_agg_fields < numfields && handleCompaction(ctx, rule, timestamp, sample,&aggVal)){
					aggvalues[2*num_agg_fields] = rule->destKey;
					aggvalues[2*num_agg_fields+1] = RedisModule_CreateStringFromDouble(ctx,aggVal);
					num_agg_fields++;
				}
				rule = rule->nextRule;
			}
		}
		put_db(ctx,rowId,pField,value);
	}

	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(ctx,"sample:%s", device);
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "XADD", "!scclcv", stream_name,"MAXLEN", "~",MAX_STREAM_LENGTH,"*" ,keyvalues, argc - 2);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeString(ctx,stream_name);
	RedisModule_FreeCallReply(reply);

	if(num_agg_fields){
		stream_name = RedisModule_CreateStringPrintf(ctx,"aggregation:%s", device);
		reply = RedisModule_Call(ctx, "XADD", "!scclcv", stream_name,"MAXLEN", "~",MAX_STREAM_LENGTH,"*" ,aggvalues, num_agg_fields*2);
		RedisModule_FreeString(ctx,stream_name);
		RedisModule_FreeCallReply(reply);
	}

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

int lmgw_pub_message(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc % 2 != 0 || argc < 4)
		return RedisModule_WrongArity(ctx);

	size_t len;
	const char *target = RedisModule_StringPtrLen(argv[1], &len);
	RedisModuleString *stream_name = RedisModule_CreateStringPrintf(ctx,
			"message:%s", target);
	RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, stream_name,
	REDISMODULE_READ | REDISMODULE_WRITE);

	int keytype = RedisModule_KeyType(stream_key);
	if (keytype != REDISMODULE_KEYTYPE_STREAM
			&& keytype != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_OK;
	}

	size_t numfields = (argc - 2) / 2;
	RedisModuleStreamID id;
	if (RedisModule_StreamAdd(stream_key, REDISMODULE_STREAM_ADD_AUTOID, &id, argv+2,
			numfields) == REDISMODULE_ERR) {
		switch (errno) {
		case ERANGE:
			RedisModule_ReplyWithError(ctx,
					"ERR the elements are too large to be stored");
			break;
		case EBADF:
			RedisModule_ReplyWithError(ctx,
					"ERR the key was not opened for writing");
			break;
		case ENOTSUP:
			RedisModule_ReplyWithError(ctx,
					"ERR the key refers to a value of a type other than stream");
			break;
		default:
			RedisModule_ReplyWithError(ctx,
					"ERR called with invalid arguments");
			break;
		}
	} else {
		RedisModuleString *str = RedisModule_CreateStringFromStreamID(ctx, &id);
		RedisModule_ReplyWithString(ctx, str);
	}

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
	if(!db_ready()){
		RedisModule_ReplyWithError(ctx,"edb is not ready");
		return REDISMODULE_ERR;
	}

	const char *device = RedisModule_StringPtrLen(argv[1], NULL);
	const char *field = RedisModule_StringPtrLen(argv[2], NULL);
	const char *strStartTime = RedisModule_StringPtrLen(argv[3], NULL);
	const char *strEndTime = RedisModule_StringPtrLen(argv[4], NULL);
	if(argc > 5){
		const char *strOrder = RedisModule_StringPtrLen(argv[5], NULL);
		query(ctx,device,field,strStartTime,strEndTime,strOrder);
	}else{
		query(ctx,device,field,strStartTime,strEndTime,"ASC");
	}

	return REDISMODULE_OK;
}

void loadingEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    if (sub == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    	RedisModule_Log(ctx, "notice", "open edb");
    	if(open_db(ctx) < 0){
    		RedisModule_Log(ctx, "warning", "edb is not ready");
    	}else{
    		RedisModule_Log(ctx, "notice", "edb is ready");
    	}
    }
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (RedisModule_Init(ctx, "lmgw", LMGW_MODULE_VERSION,
	REDISMODULE_APIVER_1) ==
	REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

    if (argc < 3) {
        RedisModule_Log(
            ctx, "warning", "Failed to parse lmgw configurations. aborting...");
        return REDISMODULE_ERR;
    }

    char *endptr = NULL;
    const char* path = RedisModule_StringPtrLen(argv[0], NULL);
	unsigned int retentionHours = strtol(RedisModule_StringPtrLen(argv[1], NULL),&endptr,10);
	unsigned int retentionMbs = strtol(RedisModule_StringPtrLen(argv[2], NULL),&endptr,10);
	int ret = init_db(ctx,path, retentionHours, retentionMbs);
	if (ret < 0) {
		printf("====open_db fail===%d\n\n", ret);
        RedisModule_Log(ctx, "warning", "Failed to open_db. ret=%d",ret);
        return REDISMODULE_ERR;
	}
	RedisModule_Log(ctx, "notice", "EDB path: %s,retentionHours: %d,retentionMbs: %d",path, retentionHours,retentionMbs);

	RedisModule_Log(ctx, "notice", "lmgw version %d", LMGW_MODULE_VERSION);
	rts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

	if (RedisModule_CreateCommand(ctx, "lmgw.pub_sample", lmgw_pub_sample_call,
			"write deny-oom", 1, 0, 1) ==
	REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.sub_sample", lmgw_sub_sample, "write",
			0, 0, -1) ==
	REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.pub_message", lmgw_pub_message_call,
			"write deny-oom", 1, 0, 1) ==
	REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.sub_message", lmgw_sub_message, "write",
			0, 0, -1) ==
	REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "lmgw.query_edb", lmgw_query, "write",
				0, 0, -1) ==
		REDISMODULE_ERR)
			return REDISMODULE_ERR;

	RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, loadingEventCallback);

	return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx){
	close_db();
}
