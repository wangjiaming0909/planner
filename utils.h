#ifndef _PLANNER_UTIL_H_
#define _PLANNER_UTIL_H_

#include "postgres.h"
#include "nodes/parsenodes.h"

const char* get_rel_kind_str(char rel_kind);

const char* get_rte_kind_str(RTEKind kind);

#endif
