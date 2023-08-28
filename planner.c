#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static PlannedStmt *my_planner(Query *parse, const char *query_string,
                               int cursorOptions, ParamListInfo boundParams) {
  PlannedStmt *stmt;
  print_query(parse);
  stmt = standard_planner(parse, query_string, cursorOptions, boundParams);
  return stmt;
}

static void create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
                        RelOptInfo *input_rel, RelOptInfo *output_rel,
                        void *extra) {
  print_planner_info(root);
}

void _PG_init(void) {
  planner_hook = my_planner;
  create_upper_paths_hook = create_upper_paths;
}
