/*
 * rule.h
 *
 *  Created on: 2023Äê9ÔÂ4ÈÕ
 *      Author: a
 */

#ifndef SRC_LMGW2_INC_RULE_H_
#define SRC_LMGW2_INC_RULE_H_
#include "module.h"
#include "compaction.h"

typedef struct CompactionRule
{
    RedisModuleString *destKey;
    timestamp_t bucketDuration;
    timestamp_t timestampAlignment;
    AggregationClass *aggClass;
    TS_AGG_TYPES_T aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
    timestamp_t startCurrentTimeBucket; // Beware that the first bucket is alway starting in 0 no
                                        // matter the alignment
} CompactionRule;


CompactionRule *NewRule(RedisModuleString *destKey,int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment);
void FreeCompactionRule(void *value);

void handleCompaction(RedisModuleCtx *ctx,CompactionRule *rule,timestamp_t timestamp,double value);
int handleQueryCompaction(CompactionRule *rule,timestamp_t timestamp,double value,double* aggVal);

int lmgw_field_create_rule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int lmgw_field_delete_rule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


#endif /* SRC_LMGW2_INC_RULE_H_ */
