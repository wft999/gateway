
/*
 *
 *  Created on: 2023Äê5ÔÂ24ÈÕ
 *      Author: a
 */

#ifndef LMGW_MODULE_VERSION

#define LMGW_VERSION_MAJOR 1
#define LMGW_VERSION_MINOR 0
#define LMGW_VERSION_PATCH 0

#define LMGW_SEMANTIC_VERSION(major, minor, patch) \
  (major * 10000 + minor * 100 + patch)

#define LMGW_MODULE_VERSION LMGW_SEMANTIC_VERSION(LMGW_VERSION_MAJOR, LMGW_VERSION_MINOR, LMGW_VERSION_PATCH)

#endif
