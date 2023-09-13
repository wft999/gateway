#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <hiredis.h>
#include "cJSON.h"
#include "mosquitto.h"


int on_message1(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *msg)
{
	redisReply *reply;
	redisContext *c = (redisContext *)userdata;

	cJSON* root = cJSON_Parse(msg->payload);
	if (root == NULL) {
		return -201;
	}

	cJSON* timeItem = cJSON_GetObjectItem(root, "time");
    if (timeItem == NULL || timeItem->valuestring == NULL || strlen(timeItem->valuestring) == 0) {
    	cJSON_Delete(root);
    	return -202;
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
        int buf_size = fieldSize*64;
        char* buf = malloc(buf_size+1);
        snprintf(buf,buf_size,"LMGW.PUB_SAMPLE %s ",deviceItem->string);
        for(int j = 0; j < fieldSize; j++){
        	cJSON *fieldItem = cJSON_GetArrayItem(deviceItem, j);
        	if (fieldItem == NULL || !cJSON_IsNumber(fieldItem)) {
        		continue;
        	}
			snprintf(buf+strlen(buf),buf_size - strlen(buf),"%s %f ",fieldItem->string,fieldItem->valuedouble);
        }
        reply = redisCommand(c,buf);
        printf("LMGW.PUB_SAMPLE[%s]: %s\n",deviceItem->string, reply->str);
        free(buf);
        freeReplyObject(reply);
    }

    cJSON_Delete(root);

	return 0;
}

int main1 (int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    unsigned int isunix = 0;
    redisContext *c;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";

    if (argc > 2) {
        if (*argv[2] == 'u' || *argv[2] == 'U') {
            isunix = 1;
            /* in this case, host is the path to the unix socket */
            printf("Will connect to unix socket @%s\n", hostname);
        }
    }

    int port = (argc > 2) ? atoi(argv[2]) : 6379;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    if (isunix) {
        c = redisConnectUnixWithTimeout(hostname, timeout);
    } else {
        c = redisConnectWithTimeout(hostname, port, timeout);
    }
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

	void* userdata = c;
	const char*	topic = "lm/upload";
	int qos = 0;
	const char*	host = "www.iotddc.com";
	port = 1887;
	const char*	client_id = NULL;
	int keepalive = 60;
	int clean_session = 1;
	const char*	username = "admin";
	const char*	password = "luomi";
	mosquitto_lib_init();
	int ret = mosquitto_subscribe_callback(on_message1,userdata,topic,qos,host,port,client_id,keepalive,clean_session,username,password,NULL,NULL);
	if(ret != MOSQ_ERR_SUCCESS){
		printf("====mosquitto_subscribe_callback fail===%d\n\n",ret);
	}

	mosquitto_lib_cleanup();

    return 0;
}

int on_message(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *msg)
{
	redisReply *reply;
	redisContext *c = (redisContext *)userdata;
	//printf("%s\\n",msg->payload);

	cJSON* root = cJSON_Parse(msg->payload);
	if (root == NULL) {
		return -201;
	}

    int deviceSize = cJSON_GetArraySize(root);
    for(int i = 0; i < deviceSize; i++){
    	cJSON *deviceItem = cJSON_GetArrayItem(root, i);
    	if (deviceItem == NULL) {
    		continue;
    	}

    	cJSON *deviceNameItem = cJSON_GetArrayItem(deviceItem, 0);
    	cJSON *propertiesItem = cJSON_GetArrayItem(deviceItem, 1);
    	int propertiesSize = cJSON_GetArraySize(propertiesItem);

    	int buf_size = propertiesSize*64;
    	char* buf = malloc(buf_size+1);
    	snprintf(buf,buf_size,"LMGW.PUB_SAMPLE %s ",deviceNameItem->valuestring);

    	for(int k = 0; k < propertiesSize; k++){
    		cJSON *propertieItem = cJSON_GetArrayItem(propertiesItem, k);
    		if (propertieItem == NULL ) {
    			continue;
    		}
    		snprintf(buf+strlen(buf),buf_size - strlen(buf),"%s %s ",propertieItem->string,propertieItem->valuestring);
    	}

    	reply = redisCommand(c,buf);
    	printf("LMGW.PUB_SAMPLE[%s]: %s\n",deviceNameItem->valuestring, reply->str);
    	free(buf);
    	freeReplyObject(reply);
    }

    cJSON_Delete(root);

	return 0;
}

int main (int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    unsigned int isunix = 0;
    redisContext *c;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";

    if (argc > 2) {
        if (*argv[2] == 'u' || *argv[2] == 'U') {
            isunix = 1;
            /* in this case, host is the path to the unix socket */
            printf("Will connect to unix socket @%s\n", hostname);
        }
    }

    int port = (argc > 2) ? atoi(argv[2]) : 6379;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    if (isunix) {
        c = redisConnectUnixWithTimeout(hostname, timeout);
    } else {
        c = redisConnectWithTimeout(hostname, port, timeout);
    }
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    redisReply *reply = redisCommand(c,"AUTH iotddc Luomiiotddc123");
    freeReplyObject(reply);

	void* userdata = c;
	const char*	topic = "/thing/property/+/+/post";
	int qos = 0;
	const char*	host = "things.iotddc.com";
	port = 1885;
	const char*	client_id = NULL;
	int keepalive = 60;
	int clean_session = 1;
	const char*	username = "iotddc";
	const char*	password = "Luomi@iotddc123";
	mosquitto_lib_init();
	int ret = mosquitto_subscribe_callback(on_message,userdata,topic,qos,host,port,client_id,keepalive,clean_session,username,password,NULL,NULL);
	if(ret != MOSQ_ERR_SUCCESS){
		printf("====mosquitto_subscribe_callback fail===%d\n\n",ret);
	}

	mosquitto_lib_cleanup();

    return 0;
}
