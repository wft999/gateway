/*
 * Copyright (c) 2019 Luomi, Inc.
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdint.h>

#include <time.h>
#include <db.h>


int main(int argc, char *argv[]) {
	COL_TYPE colType[] = {COL_TYPE_INT,COL_TYPE_INT};
	if(argc < 5){
		printf("====argc < 4====\n\n");
		goto error;
	}

	DB* pDb = (DB*)open_db(argv[1]);
	if(pDb == NULL){
		printf("====open_db:%s fail====\n\n",argv[1]);
		goto error;
	}

	void* pQuery =  open_query(pDb,argv[2], argv[3],atoi(argv[4]));

	if(pQuery == NULL){
		printf("====open_query fail====\n\n");
		goto error;
	}

	while(move_next(pQuery) == 0){

		for(int i = 0; i < pDb->pLogHead->colCount; i++){

			void* pData = fetch_col(pQuery,i);
			if(pData == NULL)
				continue;

			COL_TYPE type = pDb->pLogHead->colType[i];
			int colSize = gColTypeSize[type];

			if(type == COL_TYPE_TIMESTAMP){
				int64_t* p = (int64_t*)pData;
				char ts[30] = {0};
				struct tm* ptm;
				time_t     tt = (*p) / 1000;
				ptm = localtime(&tt);
				strftime(ts, 30, "%Y-%m-%d %H:%M:%S", ptm);
				printf("%s,",ts);

			}else if(type == COL_TYPE_INT){
				int* p = (int*)pData;
				printf("%d,",*p);
			}else if(type == COL_TYPE_STRING){
				printf("%s,",pData);
			}

		}
		printf("\n");
	}

error:
	printf("====demo end====\n\n");

	close_db(pDb);

	return 0;
}
