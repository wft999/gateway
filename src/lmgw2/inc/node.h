/*
 * node.h
 *
 *  Created on: 2023Äê7ÔÂ16ÈÕ
 *      Author: a
 */

#ifndef SRC_LMGW2_INC_NODE_H_
#define SRC_LMGW2_INC_NODE_H_

#include "module.h"

typedef struct _Node{
	RedisModuleString *keyname;
	RedisModuleString *cmd;
	RedisModuleString *run_mode;
	long long proiority;
	int pid;

	struct _Node* nextNode;
}Node,*PNode;

void LoadNode(RedisModuleCtx *ctx);
void CheckNode(RedisModuleCtx *ctx);
void WaitNode(RedisModuleCtx *ctx);
void ControlNode(RedisModuleCtx *ctx,RedisModuleString *keyname,RedisModuleString *op);
void NotifyNode(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);
void RenameNodeTo(RedisModuleCtx *ctx, RedisModuleString *keyTo);
void RenameNodeFrom(RedisModuleCtx *ctx, RedisModuleString *key);

#endif /* SRC_LMGW2_INC_NODE_H_ */
