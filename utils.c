#include "utils.h"
#include "storage/lockdefs.h"

static char rel_kind_map['z' - 'A' + 1][32] = {"",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "Partitioned Index",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "Sequence Object",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "",
                                               "Composite Type",
                                               "",
                                               "",
                                               "Foreign Table",
                                               "",
                                               "",
                                               "Secondary Index",
                                               "",
                                               "",
                                               "",
                                               "Materialized View",
                                               "",
                                               "",
                                               "Partitioned Table",
                                               "",
                                               "Ordinary Table",
                                               "",
                                               "Out of Line Values",
                                               "Unlogged Permanent Table",
                                               "View",
                                               "",
                                               "",
                                               ""};

static const char *get_rel_kind_str(char rel_kind) {
  if (rel_kind < 'A' || rel_kind > 'z')
    return "";
  return rel_kind_map[rel_kind - 'A' + 1];
}

static const char *get_rte_kind_str(RTEKind kind) {
  switch (kind) {
  case RTE_RELATION:
    return "Relation";
  case RTE_FUNCTION:
    return "Function";
  case RTE_JOIN:
    return "JOIN";
  case RTE_VALUES:
    return "Value";
  case RTE_CTE:
    return "CTE";
  case RTE_SUBQUERY:
    return "subquery";
  case RTE_TABLEFUNC:
    return "table func";
  case RTE_NAMEDTUPLESTORE:
    return "named tuple store";
  case RTE_RESULT:
    return "result";
  }
  return "";
}

static const char *get_rel_lock_mode_str(int lock_mode) {
  switch (lock_mode) {
  case NoLock:
    return "NoLock";
  case AccessShareLock:
    return "AccessSharedLock";
  case RowShareLock:
    return "RowShareLock";
  case RowExclusiveLock:
    return "RowExclusiveLock";
  case ShareUpdateExclusiveLock:
    return "ShareUpdateExclusiveLock";
  case ShareLock:
    return "ShareLock";
  case ShareRowExclusiveLock:
    return "ShareRowExclusiveLock";
  case ExclusiveLock:
    return "ExclusiveLock";
  case AccessExclusiveLock:
    return "AccessExclusiveLock";
  default:
    return "";
  }
}

static const char *get_join_type_str(JoinType join_type) {
  switch (join_type) {
  case JOIN_INNER:
    return "JOIN_INNER";
  case JOIN_LEFT:
    return "JOIN_LEFT";
  case JOIN_ANTI:
    return "JOIN_ANTI";
  case JOIN_FULL:
    return "JOIN_FULL";
  case JOIN_RIGHT:
    return "JOIN_RIGHT";
  case JOIN_SEMI:
    return "JOIN_SEMI";
  case JOIN_UNIQUE_INNER:
    return "JOIN_UNIQUE_INNER";
  case JOIN_UNIQUE_OUTER:
    return "JOIN_UNIQUE_OUTER";
  default:
    return "";
  }
}

static char *print_colnames(const List *colnames) {
  ListCell *lc;
  String *col_name;
  char *buffer = palloc(1024);
  int len = 0;
  foreach (lc, colnames) {
    col_name = lfirst_node(String, lc);
    if (len + strlen(col_name->sval) >= 1024) break;
    len += sprintf(buffer + len, "%s, ", col_name->sval);
  }
  return buffer;
}

void print_query(Query *query) {
  ListCell *lc;
  elog(INFO,
       "query id: [%ld], can set tag: [%d], has agg: [%d], has window func: "
       "[%d], has set returning funcs: [%d], has sublinks: [%d], has distinct on: [%d]",
       query->queryId, query->canSetTag, query->hasAggs, query->hasWindowFuncs,
       query->hasTargetSRFs, query->hasSubLinks, query->hasDistinctOn);
  elog(INFO, "has for update: [%d], has row secruity: [%d]", query->hasForUpdate, query->hasRowSecurity);
  foreach (lc, query->rtable) {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
    elog(INFO,
         "range table entry ref name: [%s] alias: [%s], rel_id: [%d], "
         "rel_kind: [%d:%s], "
         "rtekind: [%d:%s]",
         rte->eref ? rte->eref->aliasname : "",
         rte->alias ? rte->alias->aliasname : "", rte->relid, rte->relkind,
         get_rel_kind_str(rte->relkind), rte->rtekind,
         get_rte_kind_str(rte->rtekind));

    elog(INFO,
         "in from clause: [%d] join type: [%s], num of merged join columns: "
         "[%d]",
         rte->inFromCl, get_join_type_str(rte->jointype), rte->joinmergedcols);

    if (rte->alias) {
	  elog(INFO, "alias col: %s", print_colnames(rte->alias->colnames));
    }
    if (rte->eref) {
	  elog(INFO, "eref col: %s", print_colnames(rte->eref->colnames));
    }
    elog(INFO,
         "lateral: [%d], inh: [%d], ephemeral named relation: [%s], rel lock "
         "mode: [%s], tablesample: [%p], is from security barrier view: [%d]",
         rte->lateral, rte->inh, rte->enrname,
         get_rel_lock_mode_str(rte->rellockmode), rte->tablesample,
         rte->security_barrier);
    if (rte->rtekind == RTE_SUBQUERY) {
      elog(INFO, "-----sub query-----");
      print_query(rte->subquery);
      elog(INFO, "-----sub query-----");
    }
  }
}
