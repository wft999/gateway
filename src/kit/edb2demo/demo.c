/*
 * Copyright (c) 2019 Luomi, Inc.
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include  "db.h"
#include "mosquitto.h"


int main(int argc, char *argv[]) {
	char* buf = malloc(1024);

	int ret = open_db("/home/lighthouse/temp",72,256);
	if(ret < 0){
		printf("====open_db fail===%d\n\n",ret);
		goto error;
	}

	int device_num = 1;
	int field_num = 1;
	int count = 10;//CACHE_ROW_NUM + 1;

	while(count--){
		int64_t st = getTimestamp()/1000;
		sprintf(buf,"device0,%ld,field0=%d\n",st,1);
		printf("buf====%s\n\n",buf);
		ret = put_db(buf);
		if(ret <0){
			printf("====put_db fail====%d\n\n",ret);
			goto error;
		}
		sleep(10);
		printf("====put_db count====%d\n\n",count);
	}

	//dump("1679484112000","device0","field0");
error:
	getchar();
	free(buf);
	close_db();
	printf("====demo end====\n\n");
}

int on_message(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *msg)
{
	//printf("%s %s (%d)\n", msg->topic, (const char *)msg->payload, msg->payloadlen);
	int ret = put_db_json((char *)msg->payload);
	if(ret <0){
		printf("====put_db fail====%d\n\n",ret);
		return 1;
	}
	return 0;
}

int main1(int argc, char *argv[]) {
	int ret = open_db("/home/lighthouse/temp/compressOne",72,256);
	if(ret < 0){
		printf("====open_db fail===%d\n\n",ret);
		goto error;
	}

	//dump("1678612828","YRFD3","3yrfd.fadian3.THISNODE.M24312.F_Word0Bit0");

/*	char* out = malloc(1024*1024);
	int out_len = 1024*1024;
	ret = query("SLX12","12slx.1COC","2023-03-30 12:00:00.000", "2023-03-30 22:00:00.000",ORDER_ASC,out,out_len);
	close_db();
	int fd = open("/home/lighthouse/temp/compressOne/out.data", O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
	write(fd, out, strlen(out)-1);
	close(fd);
	free(out);
	return 0;*/

	void* userdata = NULL;
	const char*	topic = "lm/upload";
	int qos = 0;
	const char*	host = "www.iotddc.com";
	int port = 1887;
	const char*	client_id = NULL;
	int keepalive = 60;
	int clean_session = 1;
	const char*	username = "admin";
	const char*	password = "luomi";
	mosquitto_lib_init();
	ret = mosquitto_subscribe_callback(on_message,userdata,topic,qos,host,port,client_id,keepalive,clean_session,username,password,NULL,NULL);
	if(ret != MOSQ_ERR_SUCCESS){
		printf("====mosquitto_subscribe_callback fail===%d\n\n",ret);
		goto error;
	}

	error:
		mosquitto_lib_cleanup();
		close_db();
		printf("====demo end====\n\n");
		return getchar();
}

