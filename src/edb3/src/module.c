#include "redismodule.h"
#include "db.h"
#include "ttime.h"
#include "dragonbox.h"
#include "lzf.h"

#include <fcntl.h>
#include <dirent.h>
#include <inttypes.h>
#include <string.h>
#include <unistd.h>

extern DB* gdb;
extern const unsigned char true_bits[8];
extern const unsigned char false_bits[8];
#define MAX_VAL_LEN 24
void ReplyWithSample(RedisModuleCtx *ctx, u_int64_t timestamp, char* buf) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    RedisModule_ReplyWithSimpleString(ctx, buf);
}

int redis_query_block(RedisModuleCtx *ctx,FIELD *pField,CACHE* pCache,int64_t startQueryTime,int64_t endQueryTime,
		int64_t startBlockTime,int64_t endBlockTime,int blockItemNum,
		ORDER_TYPE order){

	double *sample = NULL;
	if(pField->type == FIELD_TYPE_DOUBLE && startBlockTime > pField->typeChangeTimestamp){
		ChunkIter_t *iter = Compressed_NewChunkIterator(&pCache->chunk);

		sample = malloc(sizeof(double) * blockItemNum);
		for(int i = 0; i < blockItemNum; i++){
/*
			if(!(pCache->haveTimestamp[i/8] & true_bits[i%8]))
				continue;
			if(pCache->isValueNone[i/8] & true_bits[i%8])
				continue;
*/

			Compressed_ChunkIteratorGetNext(iter, sample + i);
		}
		Compressed_FreeChunkIterator(iter);
	}


	int st = 0, end = blockItemNum,step = 1;
	if(order == ORDER_DESC){
		st = blockItemNum - 1;
		end = -1;
		step = -1;
	}

	int item_num = 0;
	while(st != end){
		int64_t t = startBlockTime + st;
		if(t >= startQueryTime && t <= endQueryTime){
	    	if(!(pCache->haveTimestamp[st/8] & true_bits[st%8])){
	    		st += step;
	    		continue;
	    	}

			if(pCache->isValueNone[st/8] & true_bits[st%8]){
				ReplyWithSample(ctx,t,"none");
			}else if(pField->type == FIELD_TYPE_BOOL || t < pField->typeChangeTimestamp){
				if(pCache->bvalue[st/8] & true_bits[st%8]){
					ReplyWithSample(ctx,t,"1");
				}else{
					ReplyWithSample(ctx,t,"0");
				}
			}else if(pField->type == FIELD_TYPE_DOUBLE){
			    char buf[MAX_VAL_LEN + 1];
			    //dragonbox_double_to_chars(sample[st], buf);
				ReplyWithSample(ctx,t,buf);
			}
			item_num++;
		}
		st += step;
	}

	if(sample) free(sample);
	return item_num;
}

int redis_query(RedisModuleCtx *ctx,const char* device,const char* field,const char* strStartTime, const char* strEndTime,ORDER_TYPE order){//2018-06-01 08:00:00.000
	int64_t startQueryTime, endQueryTime;

	if(gdb == NULL)
		return -300;

	if (taosParseTime(strStartTime, &startQueryTime, strlen(strStartTime), TSDB_TIME_PRECISION_MILLI) != 0) {
		return -302;
	}
	startQueryTime /= 1000;

	if(strcmp(strEndTime,"NOW") == 0 || strcmp(strEndTime,"now") == 0){
		endQueryTime = getTimestamp();
	}else{
		if (taosParseTime(strEndTime, &endQueryTime, strlen(strEndTime), TSDB_TIME_PRECISION_MILLI) != 0) {
			return -303;
		}
	}
	endQueryTime /= 1000;
	if(startQueryTime >=  endQueryTime){
		return -304;
	}

	char strFieldName[128];
	sprintf(strFieldName,"%s.%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		printf("field not exist!");
		return -305;
	}

	int item_num = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
	if(order == ORDER_DESC){
		CACHE* pCache = pField->cache[gdb->cacheId];
		int64_t startBlockTime = gdb->startTimestamp;
		int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
		int blockItemNum = gdb->curRowNum;
		item_num += redis_query_block(ctx,pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order);
	}


	DIR *dir = NULL;
	struct dirent *file;
	if((dir = opendir(gdb->name)) == NULL) {
		printf("opendir failed!");
		return -307;
	}

	int64_t selectedTimeStamp[MAX_QUERY_FILE];
	int curFileNum = 0;
	while((file = readdir(dir))) {
		if (file->d_type != DT_REG)
			continue;

		int len = strlen(file->d_name);
		if(len < 10 || strcmp(file->d_name + (len - 6),".index") != 0)
			continue;

		char *endptr = NULL;
		int64_t startTimeStamp = strtoll(file->d_name, &endptr, 10);
		int64_t endTimeStamp = startTimeStamp + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
		if(startTimeStamp > endQueryTime || endTimeStamp < startQueryTime)
			continue;
		if(curFileNum >= MAX_QUERY_FILE)
			break;

		int j = curFileNum;
		while(j){
			if(order == ORDER_ASC){
				if(startTimeStamp < selectedTimeStamp[j-1]){
					selectedTimeStamp[j] = selectedTimeStamp[j-1];
				}else{
					break;
				}
			}else{
				if(startTimeStamp > selectedTimeStamp[j-1]){
					selectedTimeStamp[j] = selectedTimeStamp[j-1];
				}else{
					break;
				}
			}
			j--;
		}
		selectedTimeStamp[j] = startTimeStamp;
		curFileNum++;
	}
	closedir(dir);

	char dbPathName[256];
	int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
	char* output = malloc(output_size+1);
	for(int i = 0; i < curFileNum; i++){
		sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,selectedTimeStamp[i]);
		int indexFd = open(dbPathName, O_RDWR);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,selectedTimeStamp[i]);
		int dataFd = open(dbPathName, O_RDWR);

		BLOCK block;
		read(indexFd, &block, sizeof(block));
		lseek(indexFd, pField->id * sizeof(INDEX), SEEK_CUR);
		INDEX index;
		read(indexFd, &index, sizeof(index));
		close(indexFd);
		lseek(dataFd, index.dataOffset,SEEK_SET);

		char*input = malloc(index.dataLength);
		read(dataFd, input, index.dataLength);
		close(dataFd);

		lzf_decompress(input, index.dataLength, output,output_size);

		CACHE* pCache = (CACHE*)output;
		int64_t startBlockTime = block.startTimestamp;
		int64_t endBlockTime = block.startTimestamp + block.rowCount - 1;
		int blockItemNum = block.rowCount;
		item_num += redis_query_block(ctx,pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order);
		free(input);
	}
	free(output);

	if(order == ORDER_ASC){
		CACHE* pCache = pField->cache[gdb->cacheId];
		int64_t startBlockTime = gdb->startTimestamp;
		int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
		int blockItemNum = gdb->curRowNum;
		item_num += redis_query_block(ctx,pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order);
	}
	RedisModule_ReplySetArrayLength(ctx, item_num);
	return 0;
}

int EdbQuery(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	// we must have at least 6 args
	if (argc < 5) {
		return RedisModule_WrongArity(ctx);
	}

	const char *device = RedisModule_StringPtrLen(argv[1], NULL);
	const char *field = RedisModule_StringPtrLen(argv[2], NULL);
	const char *strStartTime = RedisModule_StringPtrLen(argv[3], NULL);
	const char *strEndTime = RedisModule_StringPtrLen(argv[4], NULL);
	ORDER_TYPE order = ORDER_ASC;
	if(argc > 5){
		const char *o = RedisModule_StringPtrLen(argv[5], NULL);
		o = (strcmp(o,"desc") == 0 || strcmp(o,"DESC") == 0)? ORDER_DESC : ORDER_ASC;
	}

	int ret = redis_query(ctx,device,field,strStartTime,strEndTime,order);
	if(ret == 0){
		return REDISMODULE_OK;
	}

	char msg[64];
	sprintf(msg,"Invalid arguments,return value:%d",ret);
	RedisModule_ReplyWithError(ctx, msg);

	return REDISMODULE_ERR;
}
int EdbPutDb(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	// we must have at least 4 args
	if (argc < 2) {
		return RedisModule_WrongArity(ctx);
	}

	int ret = put_db(RedisModule_StringPtrLen(argv[1],NULL));
	if(ret == 0){
		RedisModule_ReplyWithNull(ctx);
		return REDISMODULE_OK;
	}

	char msg[64];
	sprintf(msg,"Invalid arguments,return value:%d",ret);
	RedisModule_ReplyWithError(ctx, msg);

	return REDISMODULE_ERR;
}

int EdbPutDbJson(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	// we must have at least 4 args
	if (argc < 2) {
		return RedisModule_WrongArity(ctx);
	}

	int ret = put_db_json(RedisModule_StringPtrLen(argv[1],NULL));
	if(ret == 0){
		RedisModule_ReplyWithNull(ctx);
		return REDISMODULE_OK;
	}

	char msg[64];
	sprintf(msg,"Invalid arguments,return value:%d",ret);
	RedisModule_ReplyWithError(ctx, msg);

	return REDISMODULE_ERR;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

	// Register the module itself
	if (RedisModule_Init(ctx, "edb", 1, REDISMODULE_APIVER_1) ==
	REDISMODULE_ERR) {
		return REDISMODULE_ERR;
	}

    if (argc < 3) {
        RedisModule_Log(
            ctx, "warning", "Failed to parse edb configurations. aborting...");
        return REDISMODULE_ERR;
    }

    char *endptr = NULL;
	unsigned int retentionHours = strtol(RedisModule_StringPtrLen(argv[1], NULL),&endptr,10);
	unsigned int retentionMbs = strtol(RedisModule_StringPtrLen(argv[2], NULL),&endptr,10);
	int ret = open_db(RedisModule_StringPtrLen(argv[0], NULL), retentionHours, retentionMbs);
	if (ret < 0) {
		printf("====open_db fail===%d\n\n", ret);
        RedisModule_Log(ctx, "warning", "Failed to open_db. ret=%d",ret);
        return REDISMODULE_ERR;
	}

	if (RedisModule_CreateCommand(ctx, "edb.put_db", EdbPutDb,
			"write deny-oom", 0, 0, 0) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "edb.put_db_json", EdbPutDbJson,
			"write deny-oom", 0, 0, 0) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_CreateCommand(ctx, "edb.query", EdbQuery,
				"readonly", 0, 0, 0) == REDISMODULE_ERR)
			return REDISMODULE_ERR;


	return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
	close_db();
	return REDISMODULE_OK;
}
