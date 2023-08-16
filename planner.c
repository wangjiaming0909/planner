#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static PlannedStmt *my_planner(Query *parse, const char *query_string,
                               int cursorOptions, ParamListInfo boundParams) {
  ListCell *lc;
  PlannedStmt *stmt;
  foreach (lc, parse->rtable) {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
    elog(INFO,
         "range table entry alias: [%s], rel_id: [%d], rel_kind: [%d:%s], "
         "rtekind: [%d:%s]",
         rte->alias ? rte->alias->aliasname : "", rte->relid, rte->relkind,
         get_rel_kind_str(rte->relkind), rte->rtekind,
         get_rte_kind_str(rte->rtekind));

    if (rte->alias) {
      foreach (lc, rte->alias->colnames) {
        String *col_name = lfirst_node(String, lc);
        elog(INFO, "col: [%s]", col_name->sval);
      }
    }
	if (rte->rtekind == RTE_SUBQUERY) {
	}
  }
  stmt = standard_planner(parse, query_string, cursorOptions, boundParams);
  return stmt;
}

void _PG_init(void) { planner_hook = my_planner; }
