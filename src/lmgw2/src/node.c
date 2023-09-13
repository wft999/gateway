/*
 * node.c
 *
 *  Created on: 2023Äê8ÔÂ25ÈÕ
 *      Author: a
 */
#include <sys/wait.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>

#include "node.h"
Node* pNode = NULL;
extern char **environ;
static RedisModuleString *renameNodeFromKey = NULL;
void ScanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname,RedisModuleKey *key, void *privdata){
	RedisModule_AutoMemory(ctx);
	size_t len;
	const char* keyStr = RedisModule_StringPtrLen(keyname, &len);
	if(len < 6 || strncmp(keyStr,"node:",5))
		return;

	RedisModuleKey *node_key = key;
	if(node_key == NULL)
		node_key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ);
	if (RedisModule_KeyType(node_key) != REDISMODULE_KEYTYPE_HASH) {
		if(key == NULL)
			RedisModule_CloseKey(node_key);
		return;
	}

	RedisModuleString *cmd,*run_mode,*run_proiority;
	RedisModule_HashGet(node_key, REDISMODULE_HASH_CFIELDS, "cmd", &cmd,"run_mode", &run_mode,"run_proiority", &run_proiority, NULL);

	Node *node = (Node *)RedisModule_Alloc(sizeof(Node));
	node->keyname = keyname;
	node->cmd = cmd;
	node->run_mode = run_mode;
	node->nextNode = NULL;
	node->pid = -1;
	node->proiority = 0;
	RedisModule_RetainString(ctx, keyname);
	if(cmd)
		RedisModule_RetainString(ctx, cmd);
	if(run_mode)
		RedisModule_RetainString(ctx, run_mode);
	if(run_proiority)
		RedisModule_StringToLongLong(run_proiority,&node->proiority);

	if (pNode == NULL) {
		pNode = node;
	} else {
		Node *last = pNode;
		Node *pre = NULL;
		while (last){
			if(node->proiority > last->proiority){
				if(pre)
					pre->nextNode = node;
				else
					pNode = node;

				node->nextNode = last;
				break;
			}else{
				pre = last;
				last = last->nextNode;
			}
		}

		if(last == NULL)
			pre->nextNode = node;
	}

	if(key == NULL)
		RedisModule_CloseKey(node_key);
}

int CheckNodeDone(RedisModuleCtx *ctx,Node *node) {
    int statloc = 0;
    pid_t pid;

    if(node->pid == -1)
    	return 1;

    if ((pid = waitpid(node->pid, &statloc, WNOHANG)) != 0) {
        int exitcode = WIFEXITED(statloc) ? WEXITSTATUS(statloc) : -1;
        int bysignal = 0;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

        const char* keyname = RedisModule_StringPtrLen(node->keyname, NULL);
        if (pid == -1) {
        	node->pid = -1;
            RedisModule_Log(ctx, "warning", "node[%s] waitpid() returned an error: %s.",keyname,strerror(errno));
            return 1;
        } else if (pid == node->pid) {
        	node->pid = -1;
        	RedisModule_Log(ctx, "warning", "node[%s] exit! exit code:%d,bysignal:%d",keyname,exitcode,bysignal);
        	return 1;
        } else {
            return 0;
        }
    }

    return 0;
}

void StartNode(RedisModuleCtx *ctx,Node *node){
	char *argv[10];
	size_t len;
	const char* keyname = RedisModule_StringPtrLen(node->keyname, &len);
	const char* cmd = RedisModule_StringPtrLen(node->cmd, &len);
	int i = 0;
	int start = -1;
	int argc = 0;
	while(i < len){
		if(isspace(cmd[i]) || i == len-1){
			if(start != -1){
				size_t size = i - start;
				if(i == len-1)
					size++;
				argv[argc] = RedisModule_Calloc(1,size + 1);
				strncpy(argv[argc],cmd+start,size);
				start = -1;

				argc++;
				if(argc >= 9)
					break;
			}
		}else{
			if(start == -1)
				start = i;
		}

		i++;
	}
	argv[argc] = NULL;
	if(argc == 0){
		RedisModule_Log(ctx, "warning", "node[%s] cmd format error:%s",keyname,cmd);
		return;
	}

	int pid = fork();
    if (pid == -1) {
        /* Parent (fork error). */
    	RedisModule_Log(ctx, "warning", "node[%s] fork error:%s! cmd:%s",keyname,strerror(errno),cmd);
        node->pid = -1;
    } else if (pid == 0) {
        /* Child */
        execve(argv[0],argv,environ);
        /* If we are here an error occurred. */
        _exit(2); /* Don't retry execution. */
    } else {
        node->pid = pid;
        RedisModule_Log(ctx, "notice", "node[%s] fork:%s,pid:%ld",keyname,cmd,(long)pid);
    }

    while(argc){
    	RedisModule_Free(argv[--argc]);
    }

}

void StopNode(Node *node){
	if(node->pid > 0)
		kill(node->pid,SIGKILL);
}

void LoadNode(RedisModuleCtx *ctx){
	RedisModuleScanCursor *c = RedisModule_ScanCursorCreate();
	while(RedisModule_Scan(ctx, c, ScanCallback, NULL)){
	}
	RedisModule_ScanCursorDestroy(c);
}

void CheckNode(RedisModuleCtx *ctx){
	Node *node = pNode;
	while(node){
		if(!CheckNodeDone(ctx,node)){
			node = node->nextNode;
			continue;
		}

		if(node->cmd == NULL){
			node = node->nextNode;
			continue;
		}

		size_t len;
		const char* run_mode = RedisModule_StringPtrLen(node->run_mode, &len);
		if(len != 4 || strcasecmp(run_mode,"auto")){
			node = node->nextNode;
			continue;
		}

		StartNode(ctx,node);

		node = node->nextNode;
	}
}

void FreeNode(RedisModuleCtx *ctx,Node *node){
	RedisModule_FreeString(ctx, node->keyname);
	if(node->cmd)
		RedisModule_FreeString(ctx, node->cmd);
	if(node->run_mode)
		RedisModule_FreeString(ctx, node->run_mode);
	RedisModule_Free(node);
}

void WaitNode(RedisModuleCtx *ctx){
	Node *node = pNode;
	while(node){
		if(node->pid == -1){
			node = node->nextNode;
			continue;
		}
		kill(node->pid,SIGKILL);
		node = node->nextNode;
	}

	node = pNode;
	while(node){
		if(node->pid > 0){
			CheckNodeDone(ctx,node);
		}

		Node *tmp = node;
		node = node->nextNode;

		const char* keyname = RedisModule_StringPtrLen(tmp->keyname, 0);
		RedisModule_Log(ctx, "notice", "node[%s] free,pid:%ld",keyname,(long)tmp->pid);

		FreeNode(ctx,tmp);
	}
}

void ControlNode(RedisModuleCtx *ctx,RedisModuleString *keyname,RedisModuleString *op){
	const char *cstr_keyname = RedisModule_StringPtrLen(keyname, NULL);
	const char *cstr_op = RedisModule_StringPtrLen(op, NULL);

	Node* node = pNode;
	while(node){
		const char* name = RedisModule_StringPtrLen(node->keyname, 0);
		const char* run_mode = RedisModule_StringPtrLen(node->run_mode, 0);
		if(strcmp(name,cstr_keyname) == 0){
			if(strcmp(cstr_op,"start") == 0){
				if(strcmp(run_mode,"auto") == 0){
					RedisModule_ReplyWithError(ctx,"lmgw[node control error]: node is auto mode!");
				}else if(node->pid == -1){
					StartNode(ctx,node);
					RedisModule_ReplyWithSimpleString(ctx, "OK");
				}else{
					RedisModule_ReplyWithError(ctx,"lmgw[node control error]: node is started!");
				}
			}else if(strcmp(cstr_op,"stop") == 0){
				if(node->pid == -1){
					RedisModule_ReplyWithError(ctx,"lmgw[node control error]: node is not started!");
				}else{
					StopNode(node);
					RedisModule_ReplyWithSimpleString(ctx, "OK");
				}
			}else{
				RedisModule_ReplyWithError(ctx,"lmgw[node control error]: Unknown operation!");
			}

			break;
		}

		node = node->nextNode;
	}

	if(node == NULL)
		RedisModule_ReplyWithError(ctx,"lmgw[node control error]: Unknown node!");
}

void RemoveNode(RedisModuleCtx *ctx,const char* keyStr,size_t keyLen){
	Node* node = pNode;
	Node* pre = NULL;
	while(node){
		size_t len;
		const char* keynameStr = RedisModule_StringPtrLen(node->keyname, &len);
		if(len == keyLen && strncmp(keyStr,keynameStr,len) == 0){
			if(node->pid > 0){
				StopNode(node);
				if(node->cmd){
					RedisModule_FreeString(ctx, node->cmd);
					node->cmd = NULL;
				}
			}else{
				if(pre){
					pre->nextNode = node->nextNode;
				}else{
					pNode = node->nextNode;
				}

				FreeNode(ctx,node);
			}

			break;
		}

		pre = node;
		node = node->nextNode;
	}
}

void RenameNodeFrom(RedisModuleCtx *ctx, RedisModuleString *key) {
    // keep in global variable for RenameSeriesTo() and increase recount
    RedisModule_RetainString(NULL, key);
    renameNodeFromKey = key;
}

void RenameNodeTo(RedisModuleCtx *ctx, RedisModuleString *keyTo) {
	size_t lenKeyTo;
	const char* keyToStr = RedisModule_StringPtrLen(keyTo, &lenKeyTo);
	Node* node = pNode;
	while(node){
		size_t len;
		const char* keynameStr = RedisModule_StringPtrLen(node->keyname, &len);
		if(len == lenKeyTo && strncmp(keyToStr,keynameStr,len) == 0){
		    RedisModule_FreeString(NULL, node->keyname);
		    RedisModule_RetainString(NULL, keyTo);
		    node->keyname = keyTo;

			break;
		}
		node = node->nextNode;
	}

	if(node == NULL){
		RedisModule_FreeString(NULL, renameNodeFromKey);
		renameNodeFromKey = NULL;
	}
}

void ModifyNode(RedisModuleCtx *ctx, RedisModuleString *key){
	RedisModuleKey *node_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
	if (RedisModule_KeyType(node_key) != REDISMODULE_KEYTYPE_HASH) {
		RedisModule_CloseKey(node_key);
		return;
	}
	RedisModuleString *cmd,*run_mode;
	RedisModule_HashGet(node_key, REDISMODULE_HASH_CFIELDS, "cmd", &cmd,"run_mode", &run_mode, NULL);

	size_t lenKey;
	const char* keyStr = RedisModule_StringPtrLen(key, &lenKey);
	Node* node = pNode;
	while(node){
		size_t len;
		const char* keynameStr = RedisModule_StringPtrLen(node->keyname, &len);
		if(len == lenKey && strncmp(keyStr,keynameStr,len) == 0){

			if(node->cmd){
				if(cmd == NULL){
					StopNode(node);
				}else{
					size_t len1,len2;
					const char* oldCmdStr = RedisModule_StringPtrLen(node->cmd, &len1);
					const char* newCmdStr = RedisModule_StringPtrLen(cmd, &len2);
					if(len1 != len2 || strcmp(oldCmdStr,newCmdStr) != 0){
						StopNode(node);
					}
				}

				RedisModule_FreeString(NULL, node->cmd);
			}
			if(cmd)
				RedisModule_RetainString(NULL, cmd);
		    node->cmd = cmd;

		    if(node->run_mode)
		    	RedisModule_FreeString(NULL, node->run_mode);
		    if(run_mode)
		    	RedisModule_RetainString(NULL, run_mode);
		    node->run_mode = run_mode;

			break;
		}
		node = node->nextNode;
	}

	if(node == NULL){
		node = (Node *)RedisModule_Alloc(sizeof(Node));
		node->keyname = key;
		node->cmd = cmd;
		node->run_mode = run_mode;
		node->nextNode = pNode;
		node->pid = -1;
		node->proiority = 0;
		RedisModule_RetainString(NULL, key);
		if(cmd)
			RedisModule_RetainString(NULL, cmd);
		if(run_mode)
			RedisModule_RetainString(NULL, run_mode);
		pNode = node;

	}


	RedisModule_CloseKey(node_key);
}

void NotifyNode(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
	size_t len;
	const char* keyStr = RedisModule_StringPtrLen(key, &len);
	if(len < 6 || strncmp(keyStr,"node:",5))
		return;

	if (strcasecmp(event, "del") == 0) {
		RemoveNode(ctx,keyStr,len);
		return;
	}

	if (strcasecmp(event, "rename_from") == 0) {
		RenameNodeFrom(ctx, key);
		return;
	}

	if (strcasecmp(event, "rename_to") == 0) {
		RenameNodeTo(ctx, key);
		return;
	}

	if (strcasecmp(event, "hset") == 0 || strcasecmp(event, "hdel") == 0) {
		ModifyNode(ctx, key);
		return;
	}
}
