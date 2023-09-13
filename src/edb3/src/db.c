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
#include "tscompression.h"
#include "ttime.h"
#include "cJSON.h"
#include "fast_double_parser_c.h"
#include "lzf.h"

int64_t gMillisecondOfHour = 60 * 60 * 1000;
char* field_type_name[] = {"FIELD_TYPE_BOOL","FIELD_TYPE_DOUBLE"};
const unsigned char true_bits[8] = {(1<<0),(1<<1),(1<<2),(1<<3),(1<<4),(1<<5),(1<<6),(1<<7)};
const unsigned char false_bits[8] = {~(1<<0),~(1<<1),~(1<<2),~(1<<3),~(1<<4),~(1<<5),~(1<<6),~(1<<7)};
const int cache_head_size = CACHE_BITMAP_SIZE*2 + sizeof(CompressedChunk);

DB* gdb = NULL;
const double g_bad_number = -99999;

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
    free(val);
}
void dictFieldDestructor(dict *d, void *val)
{
    UNUSED(d);
    FIELD* pField = (FIELD*)val;
	free(pField->cache[0]);
	free(pField->cache[1]);
    free(val);
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

char* process_log(char* pBuf){
	char*      lineStr = NULL;
	char*      outBuf = NULL;
	char*	 buf = pBuf;
	while((lineStr = strtok_r(buf, "\n",&outBuf)) != NULL){
		buf = NULL;
		if(*(outBuf - 1) != 0)
			break;
		else if(lineStr[0] == '{')
			_put_db_json(lineStr,1);
		else
			_put_db(lineStr,1);
	}
	return lineStr;
}

int load_log_batch(int logFd){
	size_t buf_size = 10*1024*1024;
	char* pBuf = malloc(buf_size);
	if(pBuf == NULL){
		return -401;
	}

	char* pTempBuf = pBuf;
	size_t size = buf_size - 1;
	ssize_t len = 0;
	do{
		len = read(logFd, pTempBuf,size);
		if(len < 1)
			break;

		pTempBuf[len] = 0;
		char* pReaminBuf = process_log(pBuf);
		if(pReaminBuf == NULL){
			free(pBuf);
			return -402;
		}
		if(len != size)
			break;

		size_t remain_size = buf_size -1 - (pReaminBuf - pBuf);
		memcpy(pBuf,pReaminBuf,remain_size);
		pTempBuf = pBuf + remain_size;
		size = buf_size -1 - remain_size;
	}while(1);
	free(pBuf);

	return 0;

}
int load_log(int logFd,size_t size){
	char* pBuf = malloc(size+1);
	if(pBuf == NULL){
		return -401;
	}
	read(logFd, pBuf,size);
	pBuf[size] = 0;
	process_log(pBuf);
	free(pBuf);

	return 0;

}
void *replayLog(void *arg) {
	char dbPathName[128];
	unsigned long id = (unsigned long)arg;
	sprintf (dbPathName,"%s/log%dtmp.data", gdb->name,id);
	int fd = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
	load_log_batch(fd);
	close(fd);
	unlink(dbPathName);

	int output_size = sizeof(double)*CACHE_ROW_NUM;
	char* output = malloc(output_size);

	char buf[256];
	if(gdb->fieldsChange){
		strcpy (buf, gdb->name);
		strcpy (buf + strlen (buf), "/schema.conf");
		int schemaFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

		sprintf(buf,"SYSTEM_RETENTION_HOURS=%d\n",gdb->retentionHours);
		write(schemaFd, buf, strlen(buf));
		sprintf(buf,"SYSTEM_RETENTION_MBS=%d\n",gdb->retentionMbs);
		write(schemaFd, buf, strlen(buf));

		for(int i = 0; i < gdb->nextFieldId; i++){
			FIELD* pField = gdb->pFieldsArray[i];
			if(pField->typeChangeTimestamp){
				sprintf(buf,"%s=%s,%" PRIu64 "\n",pField->name,field_type_name[pField->type],pField->typeChangeTimestamp);
			}else{
				if(pField->type == FIELD_TYPE_BOOL)
					sprintf(buf,"%s=%s\n",pField->name,field_type_name[pField->type]);
				else
					sprintf(buf,"%s=%s\n",pField->name,field_type_name[pField->type]);
			}

			write(schemaFd, buf, strlen(buf));
		}
		close(schemaFd);

		gdb->fieldsChange = 0;
	}


	sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,gdb->logStartTimestamp);
	int indexFd = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,gdb->logStartTimestamp);
	int dataFd = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

	BLOCK block;
	block.startTimestamp = gdb->logStartTimestamp;
	block.fieldCount = gdb->nextFieldId;
	block.rowCount = gdb->curLogRowNum;
	write(indexFd, &block, sizeof(block));
	for(int i = 0; i < gdb->nextFieldId; i++){
		INDEX index;
		index.dataOffset = lseek(dataFd, 0, SEEK_END);
		CACHE* pCache = gdb->pFieldsArray[i]->cache[1];

		if(gdb->pFieldsArray[i]->type == FIELD_TYPE_BOOL){
			int nelements = CACHE_BITMAP_SIZE * 3;
			index.dataLength = tsCompressStringImp((char*)pCache, nelements, output, output_size);
			write(dataFd, output, index.dataLength);

			memset(pCache,0,sizeof(CACHE));
		}else{
			int nelements = cache_head_size + pCache->chunk.idx/8 + (pCache->chunk.idx%8?1:0);
			if(output_size < (nelements + 1)){
				output_size = nelements + 1;
				output = realloc(output,output_size);
			}
			index.dataLength = lzf_compress(pCache, nelements, output, output_size);
			write(dataFd, output, index.dataLength);

			int chunk_size = pCache->chunk.size;
			memset(pCache,0,cache_head_size + chunk_size);
			pCache->chunk.size = chunk_size;
			pCache->chunk.prevLeading = 32;
			pCache->chunk.prevTrailing = 32;
		}
		write(indexFd, &index, sizeof(index));
	}
	free(output);
	close(indexFd);
	close(dataFd);

	return NULL;
}

int init_log(){
	struct stat st;
	char dbPathName[128];
	char dbPathName2[128];
	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName2, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/log1.data");
	strcpy (dbPathName2 + strlen (dbPathName2), "/log1tmp.data");
	if (stat(dbPathName, &st) == 0){
		if(st.st_size > 0){
			rename(dbPathName,dbPathName2);
			gdb->logFd[0] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

	    	pthread_attr_t attr;
	    	pthread_t thread;
	    	pthread_attr_init(&attr);
	    	void *arg = (void*)1;
	    	if(pthread_create(&thread,&attr,replayLog,arg) != 0){
	    		return -203;
	    	}
	    	pthread_detach(thread);
		}else{
			gdb->logFd[0] = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
		}
	}else{
		gdb->logFd[0] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}


	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName2, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/log2.data");
	strcpy (dbPathName2 + strlen (dbPathName2), "/log2tmp.data");
	if (stat(dbPathName, &st) == 0){
		if(st.st_size > 0){
			rename(dbPathName,dbPathName2);
			gdb->logFd[1] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

	    	pthread_attr_t attr;
	    	pthread_t thread;
	    	pthread_attr_init(&attr);
	    	void *arg = (void*)2;
	    	if(pthread_create(&thread,&attr,replayLog,arg) != 0){
	    		return -203;
	    	}
	    	pthread_detach(thread);
		}else{
			gdb->logFd[1] = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
		}
	}else{
		gdb->logFd[1] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	return 0;
}

FIELD* init_field(char* name ,FIELD_TYPE type,int64_t typeChangeTimestamp){
	FIELD* pField = (FIELD*)malloc(sizeof(FIELD));
	if(pField == NULL)
		return NULL;

	memset(pField,0,sizeof(FIELD));
	pField->name = strdup(name);

	int retval = dictAdd(gdb->fields, pField->name, pField);
	if(retval != 0){
		free(pField->name);
		free(pField);
		return NULL;
	}

	pField->type = type;
	if(type == FIELD_TYPE_DOUBLE){
		int size = cache_head_size + CHUNK_DEFAULT_SIZE;
		pField->cache[0] = calloc(1,size);
		pField->cache[0]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[0]->chunk.prevLeading = 32;
		pField->cache[0]->chunk.prevTrailing = 32;

		pField->cache[1] = calloc(1,size);
		pField->cache[1]->chunk.size = CHUNK_DEFAULT_SIZE;
		pField->cache[1]->chunk.prevLeading = 32;
		pField->cache[1]->chunk.prevTrailing = 32;

		pField->typeChangeTimestamp = typeChangeTimestamp;
	}else if(type == FIELD_TYPE_BOOL){
		int size = sizeof(CACHE);
		pField->cache[0] = calloc(1,size);
		pField->cache[1] = calloc(1,size);
	}else{
		dictDelete(gdb->fields, pField->name);
		return NULL;
	}

	pField->id = gdb->nextFieldId;
	if(gdb->nextFieldId >= gdb->maxFieldNum){
		gdb->maxFieldNum += 1000;
		gdb->pFieldsArray = realloc(gdb->pFieldsArray,sizeof(PFIELD)*gdb->maxFieldNum);
	}
	gdb->pFieldsArray[gdb->nextFieldId] = pField;
	gdb->nextFieldId++;


	return pField;
}

int init_dict(){
	gdb->fields = dictCreate(&fieldsDictType);

	struct stat st;
	char dbPathName[128];
	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/schema.conf");

	if (stat(dbPathName, &st) == 0){
		int schemaFd = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);

		char* pBuf = malloc(st.st_size+1);
		if(pBuf == NULL){
			return -101;
		}
		read(schemaFd, pBuf, st.st_size);

		char*      lineStr = NULL;
		char*      outBuf = NULL;
		char*      inBuf = NULL;
		char*	 buf = pBuf;
		while((lineStr = strtok_r(buf, "\n",&outBuf)) != NULL){
			char* name = strtok_r(lineStr, "=",&inBuf);
			char* type = strtok_r(NULL, "=",&inBuf);
			buf = NULL;
			if(strcmp(name,"SYSTEM_RETENTION_HOURS") == 0){
				//char *endptr = NULL;
				//gdb->retentionHours = strtol(type, &endptr, 10);
				continue;
			}
			if(strcmp(name,"SYSTEM_RETENTION_MBS") == 0){
				//char *endptr = NULL;
				//gdb->retentionHours = strtol(type, &endptr, 10);
				continue;
			}
			if(type == NULL){
				continue;
			}

			FIELD_TYPE t;
			int64_t typeChangeTimestamp = 0;
			if(strcmp(type,"FIELD_TYPE_BOOL") == 0){
				t = FIELD_TYPE_BOOL;
			}else if(strcmp(type,"FIELD_TYPE_DOUBLE") == 0){
				t = FIELD_TYPE_DOUBLE;
			}else{
				t = FIELD_TYPE_DOUBLE;
				char *endptr = NULL;
				typeChangeTimestamp = strtoll(type+strlen("FIELD_TYPE_DOUBLE"), &endptr, 10);
				if(endptr[0] == ',')
					typeChangeTimestamp = 0;
			}

			init_field(name,t,typeChangeTimestamp);
		}
		free(pBuf);
	}


	return 0;
}

int open_db(char* name,unsigned int retentionHours,unsigned int retentionMbs){
	struct stat st;
	if (stat(name, &st) < 0){
		return -1;
	}

	gdb = malloc(sizeof(DB));
	if(gdb == NULL){
		return -2;
	}

	memset(gdb, 0, sizeof(DB));
	gdb->signature = gdb;
	strcpy(gdb->name,name);
	gdb->gCompressType = COMPRESS_TAOS_TWO_STEP;
	gdb->retentionHours = (retentionHours > RETENTION_HOURS_MAX)?RETENTION_HOURS_MAX:retentionHours;
	gdb->retentionMbs = (retentionMbs > RETENTION_MBS_MAX)?RETENTION_MBS_MAX:retentionMbs;

	pthread_mutex_init(&(gdb->dmutex), NULL);

	gdb->pFieldsArray = malloc(sizeof(PFIELD)*DEFAULT_FIELD_NUM);
	if(gdb->pFieldsArray == NULL){
		return -3;
	}
	gdb->maxFieldNum = DEFAULT_FIELD_NUM;

	int ret = init_dict();
	if(ret < 0){
		close_db();
		return ret;
	}
	gdb->dbReady = 1;

	ret = init_log();
	if(ret < 0){
		close_db();
		return ret;
	}
	gdb->logReady = 1;

	return 0;
}
void *saveBackgroundJob(void *arg);
void close_db(){
	if(gdb && gdb->signature == gdb){
		pthread_mutex_lock(&gdb->dmutex);
		if(gdb->startTimestamp){
			gdb->preStartTimestamp = gdb->startTimestamp;
			//gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;

			saveBackgroundJob((void*)gdb->cacheId);
		}

		if(gdb->fields) dictRelease(gdb->fields);

		if(gdb->logFd[0]) {
			ftruncate(gdb->logFd[0],0);
			close(gdb->logFd[0]);
		}
		if(gdb->logFd[1]){
			ftruncate(gdb->logFd[1],0);
			close(gdb->logFd[1]);
		}
		if(gdb->indexFd) close(gdb->indexFd);
		if(gdb->dataFd) close(gdb->dataFd);
		if(gdb->pFieldsArray) free(gdb->pFieldsArray);

		pthread_mutex_unlock(&gdb->dmutex);
		pthread_mutex_destroy(&(gdb->dmutex));
		free(gdb);
		gdb = NULL;
	}

}

int64_t getTimestamp() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
}

void deleteOldFile(){
	DIR *dir = NULL;
	struct dirent *file;
	printf("deleteOldFile start!!!\n");

	if((dir = opendir(gdb->name)) == NULL) {
		printf("opendir failed!\n");
		return;
	}

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
		printf("deleteOldFile handle fiel %s!!!\n",file->d_name);

		char *endptr = NULL;
		int64_t startTimeStamp = strtoll(file->d_name, &endptr, 10);
		int64_t endTimeStamp = startTimeStamp + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;

		printf("deleteOldFile retentionHours=%d, gdb->startTimestamp - endTimeStamp=%d!!!\n",gdb->retentionHours, gdb->startTimestamp - endTimeStamp);
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
			sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,startTimeStamp);
			stat(dbPathName, &st);
			total_space += st.st_size;
			sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,startTimeStamp);
			stat(dbPathName, &st);
			total_space += st.st_size;
			continue;
		}


		sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,startTimeStamp);
		unlink(dbPathName);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,startTimeStamp);
		unlink(dbPathName);
		printf("deleteOldFile: %s!!!\n",dbPathName);
	}
	closedir(dir);

	for(int i = 0; i < curFileNum; i++){
		if(total_space < gdb->retentionMbs*1024*1024)
			break;

		struct stat st;
		sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,selectedTimeStamp[i]);
		stat(dbPathName, &st);
		total_space -= st.st_size;
		unlink(dbPathName);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,selectedTimeStamp[i]);
		stat(dbPathName, &st);
		total_space -= st.st_size;
		unlink(dbPathName);
	}

}

void *saveBackgroundJob(void *arg) {
	if(gdb == NULL || gdb->preStartTimestamp == 0)
		return NULL;

	printf("start save!!!");
	unsigned long cacheId = (unsigned long)arg;

	int output_size = sizeof(double)*CACHE_ROW_NUM;
	char* output = malloc(output_size);

	char buf[256];
	if(gdb->fieldsChange){
		strcpy (buf, gdb->name);
		strcpy (buf + strlen (buf), "/schema.conf");
		int schemaFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

		sprintf(buf,"SYSTEM_RETENTION_HOURS=%d\n",gdb->retentionHours);
		write(schemaFd, buf, strlen(buf));
		sprintf(buf,"SYSTEM_RETENTION_MBS=%d\n",gdb->retentionMbs);
		write(schemaFd, buf, strlen(buf));

		for(int i = 0; i < gdb->nextFieldId; i++){
			FIELD* pField = gdb->pFieldsArray[i];
			if(pField->typeChangeTimestamp){
				sprintf(buf,"%s=%s,%" PRIu64 "\n",pField->name,field_type_name[pField->type],pField->typeChangeTimestamp);
			}else{
				if(pField->type == FIELD_TYPE_BOOL)
					sprintf(buf,"%s=%s\n",pField->name,field_type_name[pField->type]);
				else
					sprintf(buf,"%s=%s\n",pField->name,field_type_name[pField->type]);
			}

			write(schemaFd, buf, strlen(buf));
		}
		close(schemaFd);

		gdb->fieldsChange = 0;
	}

	if(!gdb->indexFd){
		sprintf(buf,"%s/%" PRIu64 ".index",gdb->name,gdb->preStartTimestamp);
		gdb->indexFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
		sprintf(buf,"%s/%" PRIu64 ".data",gdb->name,gdb->preStartTimestamp);
		gdb->dataFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
		gdb->fileStartTimestamp = gdb->preStartTimestamp;
	}

	BLOCK block;
	block.startTimestamp = gdb->preStartTimestamp;
	block.fieldCount = gdb->nextFieldId;
	block.rowCount = (cacheId == gdb->cacheId) ? gdb->curRowNum : CACHE_ROW_NUM;
	write(gdb->indexFd, &block, sizeof(block));
	for(int i = 0; i < gdb->nextFieldId; i++){
		INDEX index;
		index.dataOffset = lseek(gdb->dataFd, 0, SEEK_END);
		CACHE* pCache = gdb->pFieldsArray[i]->cache[cacheId];

		if(gdb->pFieldsArray[i]->type == FIELD_TYPE_BOOL){
			int nelements = CACHE_BITMAP_SIZE * 3;
			index.dataLength = tsCompressStringImp((char*)pCache, nelements, output, output_size);
			write(gdb->dataFd, output, index.dataLength);

			memset(pCache,0,sizeof(CACHE));
		}else{
			int nelements = cache_head_size + pCache->chunk.idx/8 + (pCache->chunk.idx%8?1:0);
			if(output_size < (nelements + 1)){
				output_size = nelements + 1;
				output = realloc(output,output_size);
			}
			//index.dataLength = tsCompressStringImp((char*)pCache, nelements, output, output_size);
			index.dataLength = lzf_compress(pCache, nelements, output, output_size);
			write(gdb->dataFd, output, index.dataLength);

			int chunk_size = pCache->chunk.size;
			memset(pCache,0,cache_head_size + chunk_size);
			pCache->chunk.size = chunk_size;
			pCache->chunk.prevLeading = 32;
			pCache->chunk.prevTrailing = 32;
		}
		write(gdb->indexFd, &index, sizeof(index));
	}
	free(output);

	int64_t fileEndTimeStamp = gdb->fileStartTimestamp + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
	if(gdb->startTimestamp > fileEndTimeStamp){
		close(gdb->indexFd);
		close(gdb->dataFd);
		gdb->indexFd = 0;
		gdb->dataFd = 0;
		if(gdb->retentionHours > 0 || gdb->retentionMbs > 0)
			deleteOldFile();
	}

	gdb->preStartTimestamp = 0;
	ftruncate(gdb->logFd[cacheId],0);
	lseek(gdb->logFd[cacheId], 0, SEEK_SET);
	printf("finish save!!!");
	return NULL;
}

static void ensureAddSample(FIELD* pField,int cacheId, double sample) {
    ChunkResult res = Compressed_Append(&pField->cache[cacheId]->chunk, sample);
    if (res != CR_OK) {
        int oldsize = pField->cache[cacheId]->chunk.size;
        pField->cache[cacheId]->chunk.size += CHUNK_RESIZE_STEP;
        int newsize = pField->cache[cacheId]->chunk.size;
        pField->cache[cacheId] = realloc(pField->cache[cacheId], cache_head_size + newsize * sizeof(char));
        memset((char *)(pField->cache[cacheId]->chunk.data) + oldsize, 0, CHUNK_RESIZE_STEP);
        printf("%s cache%d extended to %lu \n", pField->name,cacheId,pField->cache[cacheId]->chunk.size);
        res = Compressed_Append(&pField->cache[cacheId]->chunk,sample);
        assert(res == CR_OK);
    }
}


int _put_db_json(char* pData,int cacheId){
	if(gdb == NULL || gdb->dbReady == 0)
		return -200;

	char strFieldName[128];
	pthread_mutex_lock(&gdb->dmutex);

	cJSON* root = cJSON_Parse(pData);
	if (root == NULL) {
		pthread_mutex_unlock(&gdb->dmutex);
		return -201;
	}

	cJSON* timeItem = cJSON_GetObjectItem(root, "time");
    if (timeItem == NULL || timeItem->valuestring == NULL || strlen(timeItem->valuestring) == 0) {
    	cJSON_Delete(root);
    	pthread_mutex_unlock(&gdb->dmutex);
    	return -202;
    }
    printf("timestamp=%s\n",timeItem->valuestring);
    char *endptr = NULL;
    int64_t timeStamp = strtoll(timeItem->valuestring, &endptr, 10);

    int rowId;
    if(cacheId == -1){
    	if(gdb->startTimestamp == 0){
    		gdb->startTimestamp = timeStamp;
    	}
    	rowId = timeStamp - gdb->startTimestamp;
    	gdb->curRowNum = (rowId+1)>gdb->curRowNum?(rowId+1):gdb->curRowNum;
    }else{
    	if(gdb->logStartTimestamp == 0){
    		gdb->logStartTimestamp = timeStamp;
    	}
    	rowId = timeStamp - gdb->logStartTimestamp;
    	gdb->curLogRowNum = (rowId+1)>gdb->curLogRowNum?(rowId+1):gdb->curLogRowNum;
    }

    if(rowId >= CACHE_ROW_NUM){
    	rowId = 0;
    	gdb->preStartTimestamp = gdb->startTimestamp;
    	gdb->startTimestamp = timeStamp;
    	gdb->curRowNum = 0;

    	pthread_attr_t attr;
    	pthread_t thread;

    	pthread_attr_init(&attr);
    	void *arg = (void*)gdb->cacheId;
    	gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;
    	if(pthread_create(&thread,&attr,saveBackgroundJob,arg) != 0){
    		pthread_mutex_unlock(&gdb->dmutex);
    		return -203;
    	}
    	pthread_detach(thread);
    }
	if(cacheId == -1){
		cacheId = gdb->cacheId;
		write(gdb->logFd[cacheId],pData,strlen(pData));
		write(gdb->logFd[cacheId],"\n",1);
	}
    int deviceSize = cJSON_GetArraySize(root);
    for(int i = 0; i < deviceSize; i++){
    	cJSON *deviceItem = cJSON_GetArrayItem(root, i);
    	if (deviceItem == NULL) {
    		continue;
    	}
    	if (strcmp(deviceItem->string,"time") == 0) {
    		continue;
    	}
    	if (strcmp(deviceItem->string,"clientid") == 0) {
    		continue;
    	}

        int fieldSize = cJSON_GetArraySize(deviceItem);
        for(int j = 0; j < fieldSize; j++){
        	cJSON *fieldItem = cJSON_GetArrayItem(deviceItem, j);
        	if (fieldItem == NULL || !cJSON_IsNumber(fieldItem)) {
        		continue;
        	}
			sprintf(strFieldName,"%s.%s",deviceItem->string,fieldItem->string);
			FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
			if(pField == NULL){
				if(fieldItem->valuedouble == 0){
					pField = init_field(strFieldName,FIELD_TYPE_BOOL,0);
				}else if(fieldItem->valuedouble == 1){
					pField = init_field(strFieldName,FIELD_TYPE_BOOL,0);
				}else{
					pField = init_field(strFieldName,FIELD_TYPE_DOUBLE,0);
				}
				gdb->fieldsChange = 1;
			}
			if(timeStamp < pField->curTimestamp)
				continue;

			pField->curTimestamp = timeStamp;
			CACHE* pCache = pField->cache[cacheId];
			pCache->haveTimestamp[rowId/8] += 1 << (rowId%8);
			if(fieldItem->valuedouble == g_bad_number){
				pCache->isValueNone[rowId/8] += 1 << (rowId%8);
			}else{
				if(pField->type == FIELD_TYPE_BOOL){
					if(fieldItem->valuedouble == 1){
						pCache->bvalue[rowId/8] |= true_bits[rowId%8];
					}else if(fieldItem->valuedouble == 0){
						pCache->bvalue[rowId/8] &= false_bits[rowId%8];
					}else{
						printf("%s type change\n",pField->name);
						pField->type = FIELD_TYPE_DOUBLE;
						pField->typeChangeTimestamp = gdb->startTimestamp;
						gdb->fieldsChange = 1;

						int size = cache_head_size + CHUNK_DEFAULT_SIZE;
						pField->cache[cacheId] = calloc(size,1);
						memcpy(pField->cache[cacheId],pCache,2*CACHE_BITMAP_SIZE);
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
						free(pCache);
						ensureAddSample(pField,cacheId,fieldItem->valuedouble);
					}
				}else if(pField->type == FIELD_TYPE_DOUBLE){
					if(pCache->chunk.size == 0){
						free(pCache);
						int size = cache_head_size + CHUNK_DEFAULT_SIZE;
						pField->cache[cacheId] = calloc(size,1);
						pField->cache[cacheId]->chunk.size = CHUNK_DEFAULT_SIZE;
						pField->cache[cacheId]->chunk.prevLeading = 32;
						pField->cache[cacheId]->chunk.prevTrailing = 32;
					}
					ensureAddSample(pField,cacheId,fieldItem->valuedouble);
				}
			}
        }
    }

    cJSON_Delete(root);
	pthread_mutex_unlock(&gdb->dmutex);
	return 0;
}

int _put_db(char* pData,int cacheId){
	if(gdb == NULL || gdb->dbReady == 0)
		return -200;

	pthread_mutex_lock(&gdb->dmutex);

	char*      line = NULL;
	char*      lineBuf = NULL;
	char*	   buf1 = pData;

	int replayLog = 1;
	if(cacheId == -1){
		replayLog = 0;
		cacheId = gdb->cacheId;
		write(gdb->logFd[cacheId],pData,strlen(pData));
	}

	while((line = strtok_r(buf1, "\n",&lineBuf)) != NULL){
		char* field = NULL;
		char* fieldBuf = NULL;
		char* buf2 = line;
		int i = 0;
		char* strDevice;
		int rowId;
		int64_t timeStamp;
		char strFieldName[128];

		while((field = strtok_r(buf2, ",",&fieldBuf)) != NULL){
			if(i == 0){
				strDevice = field;
			}else if(i == 1){
				char *endptr = NULL;
				timeStamp = strtoll(field, &endptr, 10);
				//timeStamp = (timeStamp / 1000) * 1000;

			    if(replayLog == 0){
			    	if(gdb->startTimestamp == 0){
			    		gdb->startTimestamp = timeStamp;
			    	}
			    	rowId = timeStamp - gdb->startTimestamp;
			    	gdb->curRowNum = (rowId+1)>gdb->curRowNum?(rowId+1):gdb->curRowNum;
			    }else{
			    	if(gdb->logStartTimestamp == 0){
			    		gdb->logStartTimestamp = timeStamp;
			    	}
			    	rowId = timeStamp - gdb->logStartTimestamp;
			    	gdb->curLogRowNum = (rowId+1)>gdb->curLogRowNum?(rowId+1):gdb->curLogRowNum;
			    }

				if(rowId >= CACHE_ROW_NUM){
					rowId = 0;
					gdb->preStartTimestamp = gdb->startTimestamp;
					gdb->startTimestamp = timeStamp;
					gdb->curRowNum = 0;

					pthread_attr_t attr;
					pthread_t thread;

					pthread_attr_init(&attr);
					void *arg = (void*)gdb->cacheId;
					gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;
					if(pthread_create(&thread,&attr,saveBackgroundJob,arg) != 0){
						pthread_mutex_unlock(&gdb->dmutex);
						return -201;
					}
					pthread_detach(thread);
					ftruncate(gdb->logFd[cacheId],0);
					lseek(gdb->logFd[cacheId], 0, SEEK_SET);

					cacheId = gdb->cacheId;
				}
			}else{
				char* inBuf = NULL;
				char* key = strtok_r(field, "=",&inBuf);
				if(key == NULL)
					break;
				char* value = strtok_r(NULL, "=",&inBuf);
				if(value == NULL)
					break;

				sprintf(strFieldName,"%s.%s",strDevice,key);
				FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
				if(pField == NULL){
					if(strcmp(value,"0") == 0){
						pField = init_field(strFieldName,FIELD_TYPE_BOOL,0);
					}else if(strcmp(value,"1") == 0){
						pField = init_field(strFieldName,FIELD_TYPE_BOOL,0);
					}else{
						pField = init_field(strFieldName,FIELD_TYPE_DOUBLE,0);
					}
					gdb->fieldsChange = 1;
				}
				if(timeStamp < pField->curTimestamp){
					i++;
					buf2 = NULL;
					continue;
				}
				pField->curTimestamp = timeStamp;
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
							printf("%s type change\n",pField->name);
							pField->type = FIELD_TYPE_DOUBLE;
							pField->typeChangeTimestamp = timeStamp;
							gdb->fieldsChange = 1;

							double sample;
							//char *endptr = NULL;
							//sample = strtod(value,&endptr);
							if ((fast_double_parser_c_parse_number(value, &sample) == NULL)) {
								pthread_mutex_unlock(&gdb->dmutex);
								return -201;
							}

							int size = cache_head_size + CHUNK_DEFAULT_SIZE;
							pField->cache[cacheId] = calloc(size,1);
							memcpy(pField->cache[cacheId]->haveTimestamp,pCache->haveTimestamp,CACHE_BITMAP_SIZE);
							memcpy(pField->cache[cacheId]->isValueNone,pCache->isValueNone,CACHE_BITMAP_SIZE);
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
							free(pCache);
							ensureAddSample(pField,cacheId,sample);
						}
					}else if(pField->type == FIELD_TYPE_DOUBLE){
						if(pCache->chunk.size == 0){
							free(pCache);
							int size = cache_head_size + CHUNK_DEFAULT_SIZE;
							pField->cache[cacheId] = calloc(size,1);
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
			}
			i++;
			buf2 = NULL;
		}
		buf1 = NULL;
	}

	pthread_mutex_unlock(&gdb->dmutex);
	return 0;
}

int dump(char* strFileName,char* device,char* field){
	if(gdb == NULL)
		return -300;

	char strFieldName[128];
	sprintf(strFieldName,"%s.%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		printf("field not exist!");
	}else{
		char dbPathName[256];
		sprintf(dbPathName,"%s/%s.index",gdb->name,strFileName);
		int indexFd = open(dbPathName, O_RDWR);
		sprintf(dbPathName,"%s/%s.data",gdb->name,strFileName);
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

		int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
		char* output = malloc(output_size+1);
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


		free(input);
		free(output);
	}

	return 0;
}

int query_block(FIELD *pField,CACHE* pCache,int64_t startQueryTime,int64_t endQueryTime,
		int64_t startBlockTime,int64_t endBlockTime,int blockItemNum,
		ORDER_TYPE order,char* out,int out_len){

	ChunkIter_t *iter = Compressed_NewChunkIterator(&pCache->chunk);

	int st = 0, end = blockItemNum,step = 1,sample_id= 0;
	if(order == ORDER_DESC){
		st = blockItemNum - 1;
		end = -1;
		step = -1;
	}

	while(st != end){
		int64_t t = startBlockTime + st;
		if(t >= startQueryTime && t <= endQueryTime){
	    	if(!(pCache->haveTimestamp[st/8] & true_bits[st%8])){
	    		st += step;
	    		continue;
	    	}

			if(pCache->isValueNone[st/8] & true_bits[st%8]){
				sprintf(out,"%" PRIu64 ",none\n",t);
			}else if(pField->type == FIELD_TYPE_BOOL || endBlockTime < pField->typeChangeTimestamp){
				if(pCache->bvalue[st/8] & true_bits[st%8]){
					sprintf(out,"%" PRIu64 ",1\n",t);
				}else{
					sprintf(out,"%" PRIu64 ",0\n",t);
				}
			}else if(pField->type == FIELD_TYPE_DOUBLE){
				double value;
				Compressed_ChunkIteratorGetNext(iter, &value);
				sprintf(out,"%" PRIu64 ",%f\n",t,value);
			}
			out += strlen(out);
			if(out - out > out_len - MIN_QUERY_BUFFER){
				return -1;
			}
		}
		st += step;
	}
	Compressed_FreeChunkIterator(iter);

	return 0;
}

int query(char* device,char* field,char* strStartTime, char* strEndTime,ORDER_TYPE order,char* out,int out_len){//2018-06-01 08:00:00.000
	int64_t startQueryTime, endQueryTime;
	char* out_src = out;

	if(gdb == NULL)
		return -300;
	if(out_len < MIN_QUERY_BUFFER)
		return 301;

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

	if(order == ORDER_DESC){
		CACHE* pCache = pField->cache[gdb->cacheId];
		int64_t startBlockTime = gdb->startTimestamp;
		int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
		int blockItemNum = gdb->curRowNum;
		int ret = query_block(pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order,out,out_len);
		if(ret == -1){
			printf("memory not enough!");
			return -306;
		}
		int cur_out_len = strlen(out);
		out += cur_out_len;
		out_len -= cur_out_len;
	}


	DIR *dir = NULL;
	struct dirent *file;
	if((dir = opendir(gdb->name)) == NULL) {
		printf("opendir failed!");
		return -307;
	}

	int haveTimeStampGap = 0;
	int64_t* selectedTimeStamp = malloc(MAX_QUERY_FILE*sizeof(int64_t));
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
//		if(curFileNum >= MAX_QUERY_FILE)
//			break;

		int j = curFileNum;
		while(j){
			if(order == ORDER_ASC){
				if(startTimeStamp < selectedTimeStamp[j-1]){
					if(j < MAX_QUERY_FILE)
						selectedTimeStamp[j] = selectedTimeStamp[j-1];
				}else{
					break;
				}
			}else{
				if(startTimeStamp > selectedTimeStamp[j-1]){
					if(j < MAX_QUERY_FILE)
						selectedTimeStamp[j] = selectedTimeStamp[j-1];
				}else{
					break;
				}
			}
			j--;
		}
		if(j < MAX_QUERY_FILE)
			selectedTimeStamp[j] = startTimeStamp;
		curFileNum++;
		if(curFileNum > MAX_QUERY_FILE){
			haveTimeStampGap = 1;
			curFileNum = MAX_QUERY_FILE;
		}
	}
	closedir(dir);

	char dbPathName[256];
	int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
	char* output = malloc(output_size+1);
	for(int i = 0; i < curFileNum && i < MAX_QUERY_FILE; i++){
		sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,selectedTimeStamp[i]);
		int indexFd = open(dbPathName, O_RDWR);
		sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,selectedTimeStamp[i]);
		int dataFd = open(dbPathName, O_RDWR);

		while(1){
			ssize_t size;
			BLOCK block;
			size = read(indexFd, &block, sizeof(block));
			if(size != sizeof(block))
				break;

			lseek(indexFd, pField->id * sizeof(INDEX), SEEK_CUR);
			INDEX index;
			size = read(indexFd, &index, sizeof(index));
			if(size != sizeof(index))
				break;
			lseek(indexFd, (block.fieldCount - pField->id - 1) * sizeof(INDEX), SEEK_CUR);
			lseek(dataFd, index.dataOffset,SEEK_SET);

			char*input = malloc(index.dataLength);
			read(dataFd, input, index.dataLength);

			//tsDecompressStringImp(input, index.dataLength, output,output_size);
			lzf_decompress(input, index.dataLength, output,output_size);

			CACHE* pCache = (CACHE*)output;
			int64_t startBlockTime = block.startTimestamp;
			int64_t endBlockTime = block.startTimestamp + block.rowCount - 1;
			int blockItemNum = block.rowCount;
			int ret = query_block(pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order,out,out_len);
			if(ret == -1){
				printf("memory not enough!");
				free(output);
				free(input);
				return -306;
			}
			free(input);
			int cur_out_len = strlen(out);
			out += cur_out_len;
			out_len -= cur_out_len;
		}
		close(indexFd);
		close(dataFd);
	}
	free(output);
	free(selectedTimeStamp);

	if(order == ORDER_ASC && haveTimeStampGap == 0){
		CACHE* pCache = pField->cache[gdb->cacheId];
		int64_t startBlockTime = gdb->startTimestamp;
		int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
		int blockItemNum = gdb->curRowNum;
		int ret = query_block(pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order,out,out_len);
		if(ret == -1){
			printf("memory not enough!");
			return -306;
		}
	}
	return strlen(out_src);
}


void load_cache(QueryContext* ctx,ResultSet* rs){
	if(rs->pCache && (rs->curFileID == 0 || rs->curFileID < MAX_QUERY_FILE)){
		free(rs->pCache);
	}
	rs->pCache = NULL;
	if(rs->curSample){
		free(rs->curSample);
		rs->curSample = NULL;
	}

	if(rs->curFileID == -1 || rs->curFileID == ctx->curFileNum){
		int64_t startCacheTime = gdb->startTimestamp;
		int64_t endCacheTime = gdb->startTimestamp + gdb->curRowNum - 1;
		if(!(startCacheTime > ctx->endQueryTime || endCacheTime < ctx->startQueryTime)){
			rs->pCache = rs->pField->cache[gdb->cacheId];
			rs->cacheRowNum = gdb->curRowNum;
			rs->startCacheTime = startCacheTime;
			rs->endCacheTime = endCacheTime;
			rs->fileBlockNum = 1;
			rs->curFileBlockID = 0;
		}
	}else if(rs->curFileID >= 0 && rs->curFileID < ctx->curFileNum){
		char dbPathName[256];
		if(rs->fileBlockNum == -1){
			sprintf(dbPathName,"%s/%" PRIu64 ".index",gdb->name,ctx->selectedTimeStamp[rs->curFileID]);
			int indexFd = open(dbPathName, O_RDWR);
			int i = 0;
			for(i = 0; i < FILE_BLOCK_NUM; i++){
				size_t len = read(indexFd, &rs->fb[i].b, sizeof(BLOCK));
				if(len != sizeof(BLOCK))
					break;
				lseek(indexFd, rs->pField->id * sizeof(INDEX), SEEK_CUR);
				len = read(indexFd, &rs->fb[i].i, sizeof(INDEX));
				if(len != sizeof(INDEX))
					break;
				lseek(indexFd, (rs->fb[i].b.fieldCount - rs->pField->id - 1) * sizeof(INDEX), SEEK_CUR);
			}
			rs->fileBlockNum = i;
			rs->curFileBlockID = ctx->order == ORDER_DESC?rs->fileBlockNum-1:0;
			close(indexFd);
		}

		if(rs->curFileBlockID >= 0 && rs->curFileBlockID < rs->fileBlockNum){
			int64_t startCacheTime = rs->fb[rs->curFileBlockID].b.startTimestamp;
			int64_t endCacheTime = rs->fb[rs->curFileBlockID].b.startTimestamp + rs->fb[rs->curFileBlockID].b.rowCount - 1;
			if(!(startCacheTime > ctx->endQueryTime || endCacheTime < ctx->startQueryTime)){
				int output_size = sizeof(CACHE) + sizeof(double)*CACHE_ROW_NUM;
				char* output = malloc(output_size+1);
				sprintf(dbPathName,"%s/%" PRIu64 ".data",gdb->name,ctx->selectedTimeStamp[rs->curFileID]);
				int dataFd = open(dbPathName, O_RDWR);
				lseek(dataFd, rs->fb[rs->curFileBlockID].i.dataOffset,SEEK_SET);
				char*input = malloc(rs->fb[rs->curFileBlockID].i.dataLength);
				read(dataFd, input, rs->fb[rs->curFileBlockID].i.dataLength);
				close(dataFd);
				lzf_decompress(input, rs->fb[rs->curFileBlockID].i.dataLength, output,output_size);

				rs->pCache = (CACHE*)output;
				rs->cacheRowNum = rs->fb[rs->curFileBlockID].b.rowCount;
				rs->startCacheTime = startCacheTime;
				rs->endCacheTime = endCacheTime;
				free(input);
			}
		}
	}

	if(rs->pCache && rs->pCache->chunk.count){
		ChunkIter_t *iter = Compressed_NewChunkIterator(&rs->pCache->chunk);
	    int numSamples = rs->pCache->chunk.count;
	    if(rs->curSample){
	    	free(rs->curSample);
	    	rs->curSample = NULL;
	    }

	    rs->curSampleSize = numSamples;
	    rs->curSample = malloc(numSamples * sizeof(double));
	    for (size_t i = 0; i < numSamples; ++i) {
	    	if(ctx->order == ORDER_ASC)
	    		Compressed_ChunkIteratorGetNext(iter, rs->curSample + i);
	    	else
	    		Compressed_ChunkIteratorGetNext(iter, rs->curSample + i);
	    }

		if(ctx->order == ORDER_ASC){
			rs->curSampleRowId = 0;
		}else{
			rs->curSampleRowId = rs->curSampleSize  - 1;
		}
	}

	if(rs->pCache){
		if(ctx->order == ORDER_ASC){
			rs->curCacheRowId = 0;
		}else{
			rs->curCacheRowId = rs->cacheRowNum - 1;
		}
	}

}

int create_result_set(QueryContext* ctx,char* device,char*field){
	ResultSet* rs = calloc(1,sizeof(ResultSet));
	rs->ctx = ctx;
	rs->pCache = NULL;
	rs->cacheRowNum = 0;
	rs->curFileID = 0;
	rs->curFileBlockID = -1;
	rs->fileBlockNum = 0;
	rs->fileBlockNum = -1;
	rs->nextResultSet = NULL;
	if(ctx->order == ORDER_ASC){
		rs->curFileID = -1;
	}else{
		rs->curFileID = ctx->curFileNum;
	}

	char strFieldName[128];
	sprintf(strFieldName,"%s.%s",device,field);
	FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
	if(pField == NULL){
		printf("field not exist!");
	}else{
		rs->pField = pField;
		load_cache(ctx,rs);
	}

    if (ctx->resultSet == NULL) {
    	ctx->resultSet = rs;
    } else {
    	ResultSet *last = ctx->resultSet;
        while (last->nextResultSet != NULL)
            last = last->nextResultSet;
        last->nextResultSet = rs;
    }

	return 0;
}

void close_query(QueryContext* ctx){
	ResultSet *rs = ctx->resultSet;
	while (rs != NULL) {
		ResultSet *nextRs = rs->nextResultSet;
		if(rs->pCache && rs->curFileID > -1 && rs->curFileID < MAX_QUERY_FILE){
			free(rs->pCache);
			free(rs);
		}
		if(rs->curSample)
			free(rs->curSample);
		rs = nextRs;
	}

	free(ctx);
}

void next_cache(QueryContext* ctx,ResultSet *rs){
	while(rs->pCache == NULL){
		rs->curFileBlockID += ctx->order;
		if(rs->curFileBlockID < 0 ||  rs->curFileBlockID >= rs->fileBlockNum){
			rs->curFileID += ctx->order;
			rs->fileBlockNum = -1;
		}
		if(rs->curFileID < 0 ||  rs->curFileID >= ctx->curFileNum){
			return;
		}

		load_cache(ctx,rs);
	}
}

int next_result(QueryContext* ctx,int resultSetId,int64_t* timeStamp,char* value){
	int id = -1;
	ResultSet *rs = ctx->resultSet;
	while (rs != NULL) {
		id++;
		if( id == resultSetId)
			break;
		rs = rs->nextResultSet;
	}
	if(rs == NULL )
		return 0;
NEXT:
	if(rs->pCache == NULL)
		next_cache(ctx,rs);
	if(rs->pCache == NULL)
		return 0;

	while(rs->curCacheRowId >= 0 && rs->curCacheRowId < rs->cacheRowNum){
		int64_t t = rs->startCacheTime + rs->curCacheRowId;
		if(t < ctx->startQueryTime || t > ctx->endQueryTime){
			rs->curCacheRowId += ctx->order;
			continue;
		}
		if(!(rs->pCache->haveTimestamp[rs->curCacheRowId/8] & true_bits[rs->curCacheRowId%8])){
			rs->curCacheRowId += ctx->order;
			continue;
		}

		*timeStamp = rs->startCacheTime + rs->curCacheRowId;
		if(rs->pCache->isValueNone[rs->curCacheRowId/8] & true_bits[rs->curCacheRowId%8]){
			strcpy(value,"none");
		}else if(rs->pField->type == FIELD_TYPE_BOOL || rs->endCacheTime < rs->pField->typeChangeTimestamp){
			if(rs->pCache->bvalue[rs->curCacheRowId/8] & true_bits[rs->curCacheRowId%8]){
				strcpy(value,"1");
			}else{
				strcpy(value,"0");
			}
		}else if(rs->pField->type == FIELD_TYPE_DOUBLE){
			sprintf(value,"%f",rs->curSample[rs->curSampleRowId]);
			rs->curSampleRowId += ctx->order;
		}

		rs->curCacheRowId += ctx->order;
		return 1;
	}
	if(rs->curFileID != -1 && rs->curFileID != ctx->curFileNum){
		free(rs->pCache);
	}
	rs->pCache = NULL;
	if(rs->curSample)
		free(rs->curSample);
	rs->curSample = NULL;

	goto NEXT;

	return 0;
}

QueryContext* open_query(char* device,char*fields,int64_t startQueryTime, int64_t endQueryTime,ORDER_TYPE order){//2018-06-01 08:00:00.000
	if(gdb == NULL)
		return -300;

	startQueryTime /= 1000;
	endQueryTime /= 1000;
	if(startQueryTime >=  endQueryTime){
		return NULL;
	}

	DIR *dir = NULL;
	struct dirent *file;
	if((dir = opendir(gdb->name)) == NULL) {
		printf("opendir failed!");
		return NULL;
	}

	QueryContext* ctx = calloc(1,sizeof(QueryContext));
	ctx->startQueryTime = startQueryTime;
	ctx->endQueryTime = endQueryTime;
	//ctx->count = count;
	ctx->order = order;
	ctx->curFileNum = 0;

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
		if(ctx->curFileNum >= MAX_QUERY_FILE)
			break;

		int j = ctx->curFileNum;
		while(j){
			if(order == ORDER_ASC){
				if(startTimeStamp < ctx->selectedTimeStamp[j-1]){
					ctx->selectedTimeStamp[j] = ctx->selectedTimeStamp[j-1];
				}else{
					break;
				}
			}else{
				if(startTimeStamp > ctx->selectedTimeStamp[j-1]){
					ctx->selectedTimeStamp[j] = ctx->selectedTimeStamp[j-1];
				}else{
					break;
				}
			}
			j--;
		}
		ctx->selectedTimeStamp[j] = startTimeStamp;
		ctx->curFileNum++;
	}
	closedir(dir);

	char* buf = fields;
	while(1){
		const char *next = strchr(buf,',');
		if(!next){
			create_result_set(ctx,device,buf);
			break;
		}

		char* field = malloc(next - buf + 1);
		strncpy(field,buf,next - buf);
		create_result_set(ctx,device,field);
		free(field);
		buf = next + 1;
	}

	return ctx;
}

int put_db(char* pData){
	_put_db(pData,-1);
}
int put_db_json(char* pData){
	_put_db_json(pData,-1);
}
