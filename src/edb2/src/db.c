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

#include "db.h"
#include "tscompression.h"
#include "ttime.h"
#include "cJSON.h"


int readFromFile(const void* handle,int64_t fileIndex);


int64_t gMillisecondOfHour = 60 * 60 * 1000;
char* field_type_name[] = {"FIELD_TYPE_BOOL","FIELD_TYPE_FLOAT","FIELD_TYPE_DOUBLE"};
const char true_bits[8] = {1,2,4,8,16,32,64,128};
const char false_bits[8] = {~1,~2,~4,~8,~16,~32,~64,~128};

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

int init_log(){
	struct stat st;
	char dbPathName[128];
	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/log1.data");
	if (stat(dbPathName, &st) == 0){
		gdb->logFd[0] = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
		ftruncate(gdb->logFd[0],0);
		lseek(gdb->logFd[0], 0, SEEK_SET);
	}else{
		gdb->logFd[0] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/log2.data");
	if (stat(dbPathName, &st) == 0){
		gdb->logFd[1] = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
		ftruncate(gdb->logFd[1],0);
		lseek(gdb->logFd[1], 0, SEEK_SET);
	}else{
		gdb->logFd[1] = open(dbPathName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	return 0;
}

int init_dict(){
	gdb->fields = dictCreate(&fieldsDictType);

	struct stat st;
	char dbPathName[128];
	strcpy (dbPathName, gdb->name);
	strcpy (dbPathName + strlen (dbPathName), "/schema.conf");

	if (stat(dbPathName, &st) == 0){
		int schemaFd = open(dbPathName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);

		char* pBuf = malloc(st.st_size);
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
			inBuf = NULL;
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

			FIELD* pField = (FIELD*)malloc(sizeof(FIELD));
			if(pField == NULL)
				return -102;

			memset(pField,0,sizeof(FIELD));
			pField->name = strdup(name);
			int retval = dictAdd(gdb->fields, pField->name, pField);
			if(retval != 0)
				return -103;

			if(strcmp(type,"FIELD_TYPE_BOOL") == 0){
				pField->type = FIELD_TYPE_BOOL;
			}else if(strcmp(type,"FIELD_TYPE_FLOAT") == 0){
				pField->type = FIELD_TYPE_DOUBLE;
			}else if(strcmp(type,"FIELD_TYPE_DOUBLE") == 0){
				pField->type = FIELD_TYPE_DOUBLE;
			}else{
				pField->type = FIELD_TYPE_DOUBLE;
				char *endptr = NULL;
				pField->typeChangeTimestamp = strtoll(type+strlen("FIELD_TYPE_DOUBLE"), &endptr, 10);
			}

			pField->id = gdb->nextFieldId;
			if(gdb->nextFieldId >= gdb->maxFieldNum){
				gdb->maxFieldNum += 1000;
				gdb->pFieldsArray = realloc(gdb->pFieldsArray,sizeof(PFIELD)*gdb->maxFieldNum);
			}
			gdb->pFieldsArray[gdb->nextFieldId] = pField;
			gdb->nextFieldId++;
		}
		free(pBuf);
	}


	return 0;
}

int open_db(char* name,unsigned int retentionHours,unsigned int retentionMbs){
	gdb = malloc(sizeof(DB));
	if(gdb == NULL){
		return -1;
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
		return -2;
	}
	gdb->maxFieldNum = DEFAULT_FIELD_NUM;

	int ret = init_dict();
	if(ret < 0){
		close_db();
		return ret;
	}

	ret = init_log();
	if(ret < 0){
		close_db();
		return ret;
	}

	return 0;
}
void *saveBackgroundJob(void *arg);
void close_db(){
	if(gdb && gdb->signature == gdb){
		if(gdb->startTimestamp){
			gdb->preStartTimestamp = gdb->startTimestamp;
			gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;

			saveBackgroundJob((void*)gdb->cacheId);
		}

		if(gdb->fields) dictRelease(gdb->fields);

		if(gdb->logFd[0]) close(gdb->logFd[0]);
		if(gdb->logFd[1]) close(gdb->logFd[1]);
		if(gdb->indexFd) close(gdb->indexFd);
		if(gdb->dataFd) close(gdb->dataFd);
		if(gdb->pFieldsArray) free(gdb->pFieldsArray);

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

	if((dir = opendir(gdb->name)) == NULL) {
		printf("opendir failed!");
		return;
	}

	char dbPathName[256];

	int64_t selectedTimeStamp[11];
	int curFileNum = 0;
	int maxFileNum = 10;
	while(file = readdir(dir)) {
		if (file->d_type != DT_REG)
			continue;

		int len = strlen(file->d_name);
		if(len < 10 || strcmp(file->d_name + (len - 6),".index") != 0)
			continue;

		char *endptr = NULL;
		int64_t startTimeStamp = strtoll(file->d_name, &endptr, 10);
		int64_t endTimeStamp = startTimeStamp + FILE_BLOCK_NUM * CACHE_ROW_NUM - 1;
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

			continue;
		}

		sprintf(dbPathName,"%s/%lld.index",gdb->name,startTimeStamp);
		unlink(dbPathName);
		sprintf(dbPathName,"%s/%lld.data",gdb->name,startTimeStamp);
		unlink(dbPathName);
	}
	closedir(dir);

	for(int i = 0; i < curFileNum; i++){
		struct stat st;
		if (stat(gdb->name, &st) == 0){
			if(st.st_size < gdb->retentionMbs*1024*1024)
				break;
			sprintf(dbPathName,"%s/%lld.index",gdb->name,selectedTimeStamp[i]);
			unlink(dbPathName);
			sprintf(dbPathName,"%s/%lld.data",gdb->name,selectedTimeStamp[i]);
			unlink(dbPathName);
		}
	}

}

void *saveBackgroundJob(void *arg) {
	if(gdb == NULL || gdb->preStartTimestamp == 0)
		return NULL;

	unsigned long cacheId = (unsigned long)arg;
	char* buffer = malloc(sizeof(double)*CACHE_ROW_NUM);
	char* output = malloc(sizeof(double)*CACHE_ROW_NUM);
	int output_size = sizeof(double)*CACHE_ROW_NUM;
	int buffer_size = sizeof(double)*CACHE_ROW_NUM;

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
			if(gdb->pFieldsArray[i]->typeChangeTimestamp){
				sprintf(buf,"%s=%s,%lld\n",gdb->pFieldsArray[i]->name,field_type_name[gdb->pFieldsArray[i]->type],gdb->pFieldsArray[i]->typeChangeTimestamp);
			}else{
				sprintf(buf,"%s=%s\n",gdb->pFieldsArray[i]->name,field_type_name[gdb->pFieldsArray[i]->type]);
			}

			write(schemaFd, buf, strlen(buf));
		}
		close(schemaFd);

		gdb->fieldsChange = 0;
	}

	if(!gdb->indexFd){
		sprintf(buf,"%s/%lld.index",gdb->name,gdb->preStartTimestamp);
		gdb->indexFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
		sprintf(buf,"%s/%lld.data",gdb->name,gdb->preStartTimestamp);
		gdb->dataFd = open(buf, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	}

	BLOCK block;
	block.startTimestamp = gdb->preStartTimestamp;
	block.fieldCount = gdb->nextFieldId;
	block.rowCount = cacheId == gdb->cacheId ? gdb->curRowNum : CACHE_ROW_NUM;
	write(gdb->indexFd, &block, sizeof(block));
	for(int i = 0; i < gdb->nextFieldId; i++){
		INDEX index;
		index.dataOffset = lseek(gdb->dataFd, 0, SEEK_END);
		char* input = (char*)(gdb->pFieldsArray[i]->cache + cacheId);
		if(gdb->pFieldsArray[i]->type == FIELD_TYPE_BOOL){
			int nelements = CACHE_BITMAP_SIZE * 2;
			index.dataLength = tsCompressTinyint(input,0,nelements,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
			write(gdb->dataFd, output, index.dataLength);
		}else if(gdb->pFieldsArray[i]->type == FIELD_TYPE_FLOAT){
			int nelements = CACHE_BITMAP_SIZE / sizeof(float) + CACHE_ROW_NUM;
			index.dataLength = tsCompressFloat(input,0,nelements,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
			write(gdb->dataFd, output, index.dataLength);
		}else{
			int nelements = CACHE_BITMAP_SIZE / sizeof(double) + CACHE_ROW_NUM;
			index.dataLength = tsCompressDouble(input,0,nelements,output,output_size,TWO_STAGE_COMP,buffer,buffer_size);
			write(gdb->dataFd, output, index.dataLength);
		}
		write(gdb->indexFd, &index, sizeof(index));
		memset(&gdb->pFieldsArray[i]->cache[cacheId],0,sizeof(CACHE));
	}
	free(buffer);
	free(output);

	gdb->curFileBlockNum++;
	if(gdb->curFileBlockNum == FILE_BLOCK_NUM){
		gdb->curFileBlockNum = 0;
		close(gdb->indexFd);
		close(gdb->dataFd);
		gdb->indexFd = 0;
		gdb->dataFd = 0;

		if(gdb->retentionHours > 0 || gdb->retentionMbs > 0)
			deleteOldFile();
	}

	gdb->preStartTimestamp = 0;
	return NULL;
}

int put_db_json(char* pData){
	if(gdb == NULL)
		return -200;

	char strFieldName[128];
	pthread_mutex_lock(&gdb->dmutex);
	cJSON* root = cJSON_Parse(pData);
	if (root == NULL) {
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
    if(gdb->startTimestamp == 0){
    	gdb->startTimestamp = timeStamp;
    }
    int rowId = timeStamp - gdb->startTimestamp;
    gdb->curRowNum = (rowId+1)>gdb->curRowNum?(rowId+1):gdb->curRowNum;
    if(rowId >= CACHE_ROW_NUM){
    	rowId = 0;
    	gdb->preStartTimestamp = gdb->startTimestamp;
    	gdb->startTimestamp = timeStamp;
    	gdb->curRowNum = 0;

    	pthread_attr_t attr;
    	pthread_t thread;
    	size_t stacksize;
    	pthread_attr_init(&attr);
    	void *arg = (void*)gdb->cacheId;
    	if(pthread_create(&thread,&attr,saveBackgroundJob,arg) != 0){
    		pthread_mutex_unlock(&gdb->dmutex);
    		return -203;
    	}
    	ftruncate(gdb->logFd[gdb->cacheId],0);
    	lseek(gdb->logFd[gdb->cacheId], 0, SEEK_SET);
    	gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;
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
				pField = (FIELD*)malloc(sizeof(FIELD));
				if(pField == NULL){
					pthread_mutex_unlock(&gdb->dmutex);
					return -204;
				}
				memset(pField,0,sizeof(FIELD));
				pField->name = strdup(strFieldName);
				int retval = dictAdd(gdb->fields,pField->name, pField);
				if(retval != 0){
					pthread_mutex_unlock(&gdb->dmutex);
					return -205;
				}

				if(fieldItem->valuedouble == 0){
					pField->type = FIELD_TYPE_BOOL;
				}else if(fieldItem->valuedouble == 1){
					pField->type = FIELD_TYPE_BOOL;
				}else{
					pField->type = FIELD_TYPE_DOUBLE;
				}

				pField->id = gdb->nextFieldId;
				if(gdb->nextFieldId >= gdb->maxFieldNum){
					gdb->maxFieldNum += 1000;
					gdb->pFieldsArray = realloc(gdb->pFieldsArray,sizeof(PFIELD)*gdb->maxFieldNum);
				}
				gdb->pFieldsArray[gdb->nextFieldId] = pField;
				gdb->nextFieldId++;
				gdb->fieldsChange = 1;
			}
			CACHE* pCache = pField->cache + gdb->cacheId;
			if(fieldItem->valuedouble == g_bad_number){
				pCache->isNone[rowId/8] += 1 << (rowId%8);
				if(pField->type == FIELD_TYPE_BOOL){
					//pCache->bvalue[rowId/8] += 1 << (rowId%8);
				}else if(pField->type == FIELD_TYPE_FLOAT){
					pCache->fvalue[rowId] = rowId > 0 ? pCache->fvalue[rowId -1] : 0;
				}else if(pField->type == FIELD_TYPE_DOUBLE){
					pCache->dvalue[rowId] = rowId > 0 ? pCache->dvalue[rowId -1] : 0;
				}
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

						pCache->dvalue[rowId] = fieldItem->valuedouble;
						for(int m = rowId - 1; m >= 0; m--){
							if(pCache->bvalue[m/8] & true_bits[m%8]){
								pCache->dvalue[m] = 1;
							}else{
								pCache->dvalue[m] = 0;
							}
						}

					}
				}else if(pField->type == FIELD_TYPE_FLOAT){
					pCache->fvalue[rowId] = fieldItem->valuedouble;
				}else if(pField->type == FIELD_TYPE_DOUBLE){
					pCache->dvalue[rowId] = fieldItem->valuedouble;
				}
			}
        }
    }

    cJSON_Delete(root);
	pthread_mutex_unlock(&gdb->dmutex);
	return 0;
}

int put_db(char* pData){
	if(gdb == NULL)
		return -200;

	pthread_mutex_lock(&gdb->dmutex);

	char*      line = NULL;
	char*      lineBuf = NULL;
	char*	   buf1 = pData;

	write(gdb->logFd[gdb->cacheId],pData,strlen(pData));
	while((line = strtok_r(buf1, "\n",&lineBuf)) != NULL){
		char* field = NULL;
		char* fieldBuf = NULL;
		char* buf2 = line;
		int i = 0;
		char* strDevice;
		int rowId;
		char strFieldName[128];

		while((field = strtok_r(buf2, ",",&fieldBuf)) != NULL){
			if(i == 0){
				strDevice = field;
			}else if(i == 1){
				char *endptr = NULL;
				int64_t timeStamp = strtoll(field, &endptr, 10);
				timeStamp = (timeStamp / 1000) * 1000;
				if(gdb->startTimestamp == 0){
					gdb->startTimestamp = timeStamp;
				}
				rowId = (timeStamp - gdb->startTimestamp)/1000;
				gdb->curRowNum = (rowId+1)>gdb->curRowNum?(rowId+1):gdb->curRowNum;
				if(rowId >= CACHE_ROW_NUM){
					rowId = 0;
					gdb->preStartTimestamp = gdb->startTimestamp;
					gdb->startTimestamp = timeStamp;
					gdb->curRowNum = 0;

					pthread_attr_t attr;
					pthread_t thread;
					size_t stacksize;
					pthread_attr_init(&attr);
					/*pthread_attr_getstacksize(&attr,&stacksize);
					while (stacksize < THREAD_STACK_SIZE) stacksize *= 2;
					    pthread_attr_setstacksize(&attr, stacksize);*/
					void *arg = (void*)gdb->cacheId;
					if(pthread_create(&thread,&attr,saveBackgroundJob,arg) != 0){
						pthread_mutex_unlock(&gdb->dmutex);
						return -201;
					}
					ftruncate(gdb->logFd[gdb->cacheId],0);
					lseek(gdb->logFd[gdb->cacheId], 0, SEEK_SET);
					gdb->cacheId = gdb->cacheId == 0 ? 1 : 0;
				}

			}else{
				char* inBuf = NULL;
				char* key = strtok_r(field, "=",&inBuf);
				char* value = strtok_r(NULL, "=",&inBuf);

				sprintf(strFieldName,"%s.%s",strDevice,key);
				FIELD *pField = dictFetchValue(gdb->fields, strFieldName);
				if(pField == NULL){
					pField = (FIELD*)malloc(sizeof(FIELD));
					if(pField == NULL){
						pthread_mutex_unlock(&gdb->dmutex);
						return -202;
					}
					memset(pField,0,sizeof(FIELD));
					pField->name = strdup(strFieldName);
					int retval = dictAdd(gdb->fields,pField->name, pField);
					if(retval != 0){
						pthread_mutex_unlock(&gdb->dmutex);
						return -203;
					}

					if(strcmp(value,"0") == 0){
						pField->type = FIELD_TYPE_BOOL;
					}else if(strcmp(value,"1") == 0){
						pField->type = FIELD_TYPE_BOOL;
					}else{
						pField->type = FIELD_TYPE_DOUBLE;
					}

					pField->id = gdb->nextFieldId;
					if(gdb->nextFieldId >= gdb->maxFieldNum){
						gdb->maxFieldNum += 1000;
						gdb->pFieldsArray = realloc(gdb->pFieldsArray,sizeof(PFIELD)*gdb->maxFieldNum);
					}
					gdb->pFieldsArray[gdb->nextFieldId] = pField;
					gdb->nextFieldId++;
					gdb->fieldsChange = 1;
				}
				CACHE* pCache = pField->cache + gdb->cacheId;
				if(strcmp(value,"none") == 0){
					pCache->isNone[rowId/8] += 1 << (rowId%8);
					if(pField->type == FIELD_TYPE_BOOL){
						//pCache->bvalue[rowId/8] += 1 << (rowId%8);
					}else if(pField->type == FIELD_TYPE_FLOAT){
						pCache->fvalue[rowId] = rowId > 0 ? pCache->fvalue[rowId -1] : 0;
					}else if(pField->type == FIELD_TYPE_DOUBLE){
						pCache->dvalue[rowId] = rowId > 0 ? pCache->dvalue[rowId -1] : 0;
					}
				}else{
					if(pField->type == FIELD_TYPE_BOOL){
						if(strcmp(value,"1") == 0){
							pCache->bvalue[rowId/8] |= true_bits[rowId%8];
						}else if(strcmp(value,"0") == 0){
							pCache->bvalue[rowId/8] &= false_bits[rowId%8];
						}else{
							printf("%s type change\n",pField->name);
							pField->type = FIELD_TYPE_DOUBLE;
							pField->typeChangeTimestamp = gdb->startTimestamp;
							gdb->fieldsChange = 1;

							char *endptr = NULL;
							pCache->dvalue[rowId] = strtod(value,&endptr);
							for(int m = rowId - 1; m >= 0; m--){
								if(pCache->bvalue[m/8] & true_bits[m%8]){
									pCache->dvalue[m] = 1;
								}else{
									pCache->dvalue[m] = 0;
								}
							}
						}
					}else if(pField->type == FIELD_TYPE_FLOAT){
						char *endptr = NULL;
						pCache->fvalue[rowId] = strtof(value,&endptr);
					}else if(pField->type == FIELD_TYPE_DOUBLE){
						char *endptr = NULL;
						pCache->dvalue[rowId] = strtod(value,&endptr);
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
	int64_t startTime, endTime;

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

		int output_size = sizeof(CACHE);
		int buffer_size = sizeof(CACHE);
		char* output = malloc(output_size);
		char* buffer = malloc(buffer_size);


		if(pField->type == FIELD_TYPE_BOOL){
			int nelements = CACHE_BITMAP_SIZE * 2;
			tsDecompressTinyint(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}else if(pField->type == FIELD_TYPE_FLOAT){
			int nelements = CACHE_BITMAP_SIZE / sizeof(float) + CACHE_ROW_NUM;
			tsDecompressFloat(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}else if(pField->type == FIELD_TYPE_DOUBLE){
			int nelements = CACHE_BITMAP_SIZE / sizeof(double) + CACHE_ROW_NUM;
			tsDecompressDouble(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}

		CACHE* pCache = (CACHE*)output;
		for(int i = 0; i < CACHE_ROW_NUM; i++){
			if(pField->type == FIELD_TYPE_BOOL){
				if(pCache->bvalue[i/8] & true_bits[i%8])
					printf("On\n");
				else
					printf("Off\n");
			}else if(pField->type == FIELD_TYPE_FLOAT){
				printf("%f\n",pCache->fvalue[i]);
			}else if(pField->type == FIELD_TYPE_DOUBLE){
				printf("%f\n",pCache->dvalue[i]);
			}

		}

		free(input);
		free(output);
		free(buffer);
	}

	return 0;
}

int query_block(FIELD *pField,CACHE* pCache,int64_t startQueryTime,int64_t endQueryTime,
		int64_t startBlockTime,int64_t endBlockTime,int blockItemNum,
		ORDER_TYPE order,char* out,int out_len){

	int st = 0, end = blockItemNum,step = 1;
	if(order == ORDER_DESC){
		st = blockItemNum - 1;
		end = -1;
		step = -1;
	}

	while(st != end){
		int64_t t = startBlockTime + st;
		if(t >= startQueryTime && t <= endQueryTime){
			if(pCache->isNone[st/8] & true_bits[st%8]){
				sprintf(out,"%lld,none\n",t);
			}else if(pField->type == FIELD_TYPE_BOOL || t < pField->typeChangeTimestamp){
				if(pCache->bvalue[st/8] & true_bits[st%8]){
					sprintf(out,"%lld,1\n",t);
				}else{
					sprintf(out,"%lld,0\n",t);
				}
			}else if(pField->type == FIELD_TYPE_FLOAT){
				sprintf(out,"%lld,%f\n",t,pCache->fvalue[st]);
			}else if(pField->type == FIELD_TYPE_DOUBLE){
				sprintf(out,"%lld,%f\n",t,pCache->dvalue[st]);
			}
			out += strlen(out);
			if(out - out > out_len - MIN_QUERY_BUFFER)
				return -1;
		}
		st += step;
	}

	return 0;
}

int query(char* device,char* field,char* strStartTime, char* strEndTime,ORDER_TYPE order,char* out,int out_len){//2018-06-01 08:00:00.000
	int64_t startQueryTime, endQueryTime;

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

	if(order == ORDER_ASC){
		CACHE* pCache = pField->cache + gdb->cacheId;
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

	int64_t selectedTimeStamp[MAX_QUERY_FILE];
	int curFileNum = 0;
	while(file = readdir(dir)) {
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
	int output_size = sizeof(CACHE);
	int buffer_size = sizeof(CACHE);
	char* output = malloc(output_size);
	char* buffer = malloc(buffer_size);
	for(int i = 0; i < curFileNum; i++){
		sprintf(dbPathName,"%s/%lld.index",gdb->name,selectedTimeStamp[i]);
		int indexFd = open(dbPathName, O_RDWR);
		sprintf(dbPathName,"%s/%lld.data",gdb->name,selectedTimeStamp[i]);
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

		if(pField->type == FIELD_TYPE_BOOL || block.startTimestamp < pField->typeChangeTimestamp){
			int nelements = CACHE_BITMAP_SIZE * 2;
			tsDecompressTinyint(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}else if(pField->type == FIELD_TYPE_FLOAT){
			int nelements = CACHE_BITMAP_SIZE / sizeof(float) + CACHE_ROW_NUM;
			tsDecompressFloat(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}else if(pField->type == FIELD_TYPE_DOUBLE){
			int nelements = CACHE_BITMAP_SIZE / sizeof(double) + CACHE_ROW_NUM;
			tsDecompressDouble(input, index.dataLength, nelements, output,output_size, TWO_STAGE_COMP, buffer, buffer_size);
		}

		CACHE* pCache = (CACHE*)output;
		int64_t startBlockTime = block.startTimestamp;
		int64_t endBlockTime = block.startTimestamp + block.rowCount - 1;
		int blockItemNum = block.rowCount;
		int ret = query_block(pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order,out,out_len);
		if(ret == -1){
			printf("memory not enough!");
			free(output);
			free(buffer);
			free(input);
			return -306;
		}
		free(input);
		int cur_out_len = strlen(out);
		out += cur_out_len;
		out_len -= cur_out_len;
	}
	free(output);
	free(buffer);

	if(order == ORDER_DESC){
		CACHE* pCache = pField->cache + gdb->cacheId;
		int64_t startBlockTime = gdb->startTimestamp;
		int64_t endBlockTime = gdb->startTimestamp + gdb->curRowNum - 1;
		int blockItemNum = gdb->curRowNum;
		int ret = query_block(pField,pCache,startQueryTime,endQueryTime,startBlockTime,endBlockTime,blockItemNum,order,out,out_len);
		if(ret == -1){
			printf("memory not enough!");
			return -306;
		}
	}
	return 0;
}
