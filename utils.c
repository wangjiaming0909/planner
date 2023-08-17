#include "utils.h"
#include "storage/lockdefs.h"
#include <jansson.h>

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

static json_t* node_to_json(const Node* n);
static json_t* query_to_json(const Query* query);

static json_t* rte_to_json(const RangeTblEntry* rte);

static json_t *list_to_json(List *l) {
  json_t *list_json = json_array();
  ListCell *cell;
  foreach (cell, l) {
    Node *cn = lfirst(cell);
    json_array_append(list_json, node_to_json(cn));
  }
  return list_json;
}

static json_t *node_to_json(const Node *n) {
  json_t *ret;
  if (!n) return json_null();
  switch (n->type) {
  case T_Alias: {
    Alias *alias = (Alias *)n;
    ret = json_pack("{}");
    json_object_set(ret, "name", json_string(alias->aliasname));
    json_object_set(ret, "cols", list_to_json(alias->colnames));
    break;
  }
  case T_String: {
    String *str = (String *)n;
    ret = json_string(str->sval);
    break;
  }
  case T_RangeTblEntry: {
    ret = rte_to_json((RangeTblEntry *)n);
    break;
  }
  default:
    ret = json_null();
    break;
  }
  return ret;
}

static json_t* rte_to_json(const RangeTblEntry* rte) {
  json_t *rte_json = json_pack("{}");
  json_object_set(rte_json, "eref alias", node_to_json((Node*)rte->eref));
  json_object_set(rte_json, "alias", node_to_json((Node*)rte->alias));

  json_object_set(rte_json, "rel id", json_integer(rte->relid));

  json_object_set(rte_json, "rel kind", json_integer(rte->relkind));
  json_object_set(rte_json, "rel kind str", json_string(get_rel_kind_str(rte->relkind)));

  json_object_set(rte_json, "rte kind", json_integer(rte->rtekind));
  json_object_set(rte_json, "rte kind str", json_string(get_rte_kind_str(rte->rtekind)));

  json_object_set(rte_json, "in from clause", json_integer(rte->inFromCl));
  json_object_set(rte_json, "join type", json_string(get_join_type_str(rte->jointype)));
  json_object_set(rte_json, "num of merged join columns", json_integer(rte->joinmergedcols));

  json_object_set(rte_json, "lateral", json_integer(rte->lateral));
  json_object_set(rte_json, "inh", json_integer(rte->inh));
  json_object_set(rte_json, "ephemeral named relation", json_string(rte->enrname));
  json_object_set(rte_json, "rel lock mode", json_string(get_rel_lock_mode_str(rte->rellockmode)));
  //json_object_set(rte_json, "table sample", json_t *value);
  json_object_set(rte_json, "is from security barrier view", json_integer(rte->security_barrier));

  if (rte->rtekind == RTE_SUBQUERY) {
    json_t *subquery = query_to_json(rte->subquery);
    json_object_set(rte_json, "subquery", subquery);
  }

  return rte_json;
}

static void print_from_expr(const FromExpr *from_expr, const char *prefix) {
  ListCell *lc;
  int i;
  i = 0;
  foreach (lc, from_expr->fromlist) {
    elog(INFO, "%s--from expr:%d", prefix, i++);
    Node *n = lfirst_node(Node, lc);
    if (IsA(n, RangeTblRef)) {
      RangeTblRef *rtr = (RangeTblRef *)n;
      elog(INFO, "%srange tbl ref to: %d", prefix, rtr->rtindex);
    } else if (IsA(n, JoinExpr)) {
      JoinExpr *je = (JoinExpr *)n;
      elog(INFO, "%sjoin expr jointype: %s, is Natural: %d", prefix,
           get_join_type_str(je->jointype), je->isNatural);
    } else {
      elog(INFO, "%ssome type of expr in from expr not handled: [%d]", prefix,
           n->type);
    }
  }
}

static json_t* query_to_json(const Query* query) {
  ListCell *lc;
  json_t *json = json_pack("{}");
  json_t *rtable_list = json_array();

  json_object_set(json, "query_id", json_integer(query->queryId));
  json_object_set(json, "can_set_tag", json_boolean(query->canSetTag));
  json_object_set(json, "has_agg", json_boolean(query->hasAggs));
  json_object_set(json, "has_window_func", json_boolean(query->hasWindowFuncs));
  json_object_set(json, "has_set_returning_funcs", json_boolean(query->hasTargetSRFs));
  json_object_set(json, "has_sublinks", json_boolean(query->hasSubLinks));
  json_object_set(json, "has distinct on", json_boolean(query->hasDistinctOn));
  json_object_set(json, "has for update", json_boolean(query->hasForUpdate));
  json_object_set(json, "has row security", json_boolean(query->hasRowSecurity));

  json_object_set(json, "rtable", rtable_list);
  foreach(lc, query->rtable) {
    json_array_append(rtable_list, node_to_json(lfirst_node(Node, lc)));
  }
  return json;
}

void print_query(Query *query, const char *prefix) {

  json_t *json = query_to_json(query);
  elog(INFO, "%s", json_dumps(json, JSON_INDENT(2)));
  json_decref(json);
}
