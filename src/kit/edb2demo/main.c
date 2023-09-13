#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>
#include <time.h>
#include "db.h"

#if !defined(_WIN32)
#include <unistd.h>
#else

#include <windows.h>

#endif

#if defined(_WRS_KERNEL)
#include <OsWrapper.h>
#endif


int main(int argc, char *argv[]) {

	//int ret = open_db("/home/temp", 12, 1024);
	int ret = open_db(argv[1],12,1024);
    if (ret < 0) {
        printf("====open_db fail===%d\n\n", ret);
        return ret;
    }
    

    //sleep(60);
    int out_len = 1024*1024;
    char* out = malloc(1024*1024);
    memset(out, 0, out_len);
    //char* start = "2023-07-27 20:00:00.000";
    //char* end = "2023-07-28 9:00:00.000";
    ret = query("Device1", "tag0004", argv[2], argv[3], ORDER_ASC,out,out_len);
    printf("path=%s,start=%s,end=%s\n", argv[1],argv[2],argv[3]);
    printf("%s", out);

/*    QueryContext* ctx = open_query("Device1","tag0001,tag0002,tag0004",1690351740000, 1690415631000,ORDER_ASC);
    int64_t timeStamp;
    char value[64];
    while(next_result(ctx,0,&timeStamp,value)){
    	printf("tag0001,timeStamp=%lld,value=%s\n",timeStamp,value);
    }
    printf("-------------------------------------------------------------\n");
    while(next_result(ctx,1,&timeStamp,value)){
    	printf("tag0002,timeStamp=%lld,value=%s\n",timeStamp,value);
    }
    printf("-------------------------------------------------------------\n");
    while(next_result(ctx,2,&timeStamp,value)){
    	printf("tag0004,timeStamp=%lld,value=%s\n",timeStamp,value);
    }
    close_query(ctx);*/

    close_db();
    //free(out);
    return ret;
}

