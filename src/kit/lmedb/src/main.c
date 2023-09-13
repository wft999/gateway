#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include <hiredis.h>
#include <async.h>
#include <ae_adapters.h>

/* Put event loop in the global scope, so it can be explicitly stopped */
static aeEventLoop *loop;
double field1_value = 2;
double field2_value = 3;
int count = 8000;
void configCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL){
    	redisAsyncDisconnect(c);
    	return;
    }

    char *endptr = NULL;
    char* path = reply->element[0]->str;
    unsigned int retentionHours = strtol(reply->element[1]->str,&endptr,10);
    unsigned int retentionMbs = strtol(reply->element[2]->str,&endptr,10);
}

void messageCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL){
    	redisAsyncDisconnect(c);
    	return;
    }

    if(REDIS_REPLY_NIL != reply->type)
    	printf("msg: %d\n",  reply->type);
    redisAsyncCommand(c, messageCallback, NULL,"LMGW.SUB_MESSAGE edb");
}

void sampleCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL){
    	redisAsyncDisconnect(c);
    	return;
    }

    if(REDIS_REPLY_ARRAY == reply->type){
    	for(size_t i = 0; i < reply->elements; i++){
    		char *device = reply->element[i]->element[0]->str + 7;
    		redisReply *reply1 = reply->element[i]->element[1];
    		for(size_t j = 0; j < reply1->elements; j++){
    			int64_t timeStamp;
    			unsigned int sn;
    			sscanf(reply1->element[j]->element[0]->str,"%" PRIu64 "-%d",&timeStamp,&sn);

    			printf("sample[%s]: %" PRIu64 "\n",device, timeStamp);
    			redisReply *reply2 = reply1->element[j]->element[1];
    			for(size_t k = 0; k < reply2->elements; k+=2){
    				char *field = reply2->element[k]->str;
    				char* value = reply2->element[k+1]->str;
    			}
    		}
    	}
    }

    redisAsyncCommand(c, sampleCallback, NULL,"LMGW.SUB_SAMPLE edb");
}

void pubSampleCallback(redisAsyncContext *c, void *r, void *privdata) {
	if(count < 0)
		return;

	sleep(1);
	struct timeval systemTime;
	gettimeofday(&systemTime, NULL);
	uint64_t t= (uint64_t)systemTime.tv_sec * 1000LL + (uint64_t)systemTime.tv_usec / 1000;
    field1_value += 1.0;
    field2_value += 1.0;
    count--;
    printf("timestamp:%" PRIu64 " field1_value: %f,field2_value: %f\n", t,field1_value,field2_value);
    redisAsyncCommand(c, pubSampleCallback, NULL, "LMGW.PUB_SAMPLE %" PRIu64 " device1 field1 %f field2 %f",t,field1_value,field2_value);
}

void authCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL || REDIS_REPLY_ERROR == reply->type){
    	redisAsyncDisconnect(c);
    	return;
    }
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    uint64_t t= (uint64_t)systemTime.tv_sec * 1000LL + (uint64_t)systemTime.tv_usec / 1000;
    redisAsyncCommand(c, pubSampleCallback, NULL, "LMGW.PUB_SAMPLE %" PRIu64 " device1 field1 %f field2 %f",t,field1_value,field2_value);

}

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        aeStop(loop);
        return;
    }
    //redisAsyncCommand(c, authCallback, NULL, "AUTH iotddc Luomiiotddc123");
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    uint64_t t= (uint64_t)systemTime.tv_sec * 1000LL + (uint64_t)systemTime.tv_usec / 1000;
    redisAsyncCommand(c, pubSampleCallback, NULL, "LMGW.PUB_SAMPLE %" PRIu64 " device1 field1 %f field2 %f",t,field1_value,field2_value);

    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        aeStop(loop);
        return;
    }

    printf("Disconnected...\n");
    aeStop(loop);
}

int main (int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    unsigned int isunix = 0;
    redisAsyncContext *c;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";

    if (argc > 2) {
        if (*argv[2] == 'u' || *argv[2] == 'U') {
            isunix = 1;
            /* in this case, host is the path to the unix socket */
            printf("Will connect to unix socket @%s\n", hostname);
        }
    }

    int port = (argc > 2) ? atoi(argv[2]) : 6378;
    if (isunix) {
        c = redisAsyncConnectUnix(hostname);
    } else {
        c = redisAsyncConnect(hostname, port);
    }
    if (c->err) {
    	printf("Connection error: %s\n", c->errstr);
        return 1;
    }
    //redisAsyncCommand(c,"AUTH iotddc Luomiiotddc123");


    loop = aeCreateEventLoop(64);
    redisAeAttach(loop, c);
    redisAsyncSetConnectCallback(c,connectCallback);
    redisAsyncSetDisconnectCallback(c,disconnectCallback);
    //redisAsyncCommand(c, configCallback, NULL, "HMGET node:edb path retentionHours retentionMbs");
    //redisAsyncCommand(c, sampleCallback, NULL, "LMGW.SUB_SAMPLE edb");
    //redisAsyncCommand(c, messageCallback, NULL,"LMGW.SUB_MESSAGE edb");
    aeMain(loop);
    return 0;
}

