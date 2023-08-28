#ifndef _PLANNER_UTIL_H_
#define _PLANNER_UTIL_H_

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"

void print_query(const Query* query);
void print_planner_info(const PlannerInfo* root);

#endif
