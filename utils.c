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

static const char *get_bool_expr_type_str(BoolExprType type) {
  switch (type) {
  case AND_EXPR:
    return "AND";
  case OR_EXPR:
    return "OR";
  case NOT_EXPR:
    return "NOT";
  }
  return "unknown bool expr";
}

static json_t *bitmapset_to_json(const Bitmapset *bm) {
  json_t* arr = json_array();
  int x = -1;
  while ((x = bms_next_member(bm, x)) >= 0) {
    json_array_append(arr, json_integer(x));
  }
  return arr;
}

static json_t *node_to_json(const Node *n);
static json_t *query_to_json(const Query *query);

static json_t *rte_to_json(const RangeTblEntry *rte);

static json_t *list_to_json(List *l) {
  json_t *list_json = json_array();
  ListCell *cell;
  foreach (cell, l) {
    Node *cn = lfirst(cell);
    json_array_append(list_json, node_to_json(cn));
  }
  return list_json;
}

static json_t *rte_to_json(const RangeTblEntry *rte) {
  json_t *rte_json = json_pack("{}");
  json_object_set(rte_json, "type", json_string("rte"));
  json_object_set(rte_json, "eref alias", node_to_json((Node *)rte->eref));
  json_object_set(rte_json, "alias", node_to_json((Node *)rte->alias));

  json_object_set(rte_json, "rel id", json_integer(rte->relid));

  json_object_set(rte_json, "rel kind", json_integer(rte->relkind));
  json_object_set(rte_json, "rel kind str",
                  json_string(get_rel_kind_str(rte->relkind)));

  json_object_set(rte_json, "rte kind", json_integer(rte->rtekind));
  json_object_set(rte_json, "rte kind str",
                  json_string(get_rte_kind_str(rte->rtekind)));

  json_object_set(rte_json, "in from clause", json_integer(rte->inFromCl));
  json_object_set(rte_json, "join type",
                  json_string(get_join_type_str(rte->jointype)));
  json_object_set(rte_json, "num of merged join columns",
                  json_integer(rte->joinmergedcols));

  json_object_set(rte_json, "lateral", json_integer(rte->lateral));
  json_object_set(rte_json, "inh", json_integer(rte->inh));
  json_object_set(rte_json, "ephemeral named relation",
                  json_string(rte->enrname));
  json_object_set(rte_json, "rel lock mode",
                  json_string(get_rel_lock_mode_str(rte->rellockmode)));
  // json_object_set(rte_json, "table sample", json_t *value);
  json_object_set(rte_json, "is from security barrier view",
                  json_integer(rte->security_barrier));

  if (rte->rtekind == RTE_SUBQUERY) {
    json_t *subquery = query_to_json(rte->subquery);
    json_object_set(rte_json, "subquery", subquery);
  }

  return rte_json;
}

static json_t *from_expr_to_json(const FromExpr *expr) {
  json_t *expr_json = json_pack("{}");
  json_object_set(expr_json, "type", json_string("FromExpr"));
  json_object_set(expr_json, "from list refs to rt",
                  list_to_json(expr->fromlist));
  json_object_set(expr_json, "quals", node_to_json(expr->quals));
  return expr_json;
}

static json_t *rtr_to_json(const RangeTblRef *rtr) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("rtr"));
  json_object_set(ret, "rtindex", json_integer(rtr->rtindex));
  return ret;
}

static json_t *join_expr_to_json(const JoinExpr *expr) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("JoinExpr"));
  json_object_set(ret, "JoinType",
                  json_string(get_join_type_str(expr->jointype)));
  json_object_set(ret, "IsNatural", json_boolean(expr->isNatural));
  json_object_set(ret, "left", node_to_json(expr->larg));
  json_object_set(ret, "right", node_to_json(expr->rarg));
  json_object_set(ret, "usingClause", list_to_json(expr->usingClause));
  json_object_set(ret, "joinUsingAlias",
                  node_to_json((Node *)expr->join_using_alias));
  json_object_set(ret, "quals", node_to_json(expr->quals));
  json_object_set(ret, "alias", node_to_json((Node *)expr->alias));
  json_object_set(ret, "rtindex", json_integer(expr->rtindex));
  return ret;
}

static json_t *oid_to_json(Oid oid) { return json_integer(oid); }

static json_t *bool_expr_to_json(const BoolExpr *expr) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("BoolExpr"));
  // json_object_set(ret, "expr", node_to_json((Node *)&expr->xpr));
  json_object_set(ret, "oper",
                  json_string(get_bool_expr_type_str(expr->boolop)));
  json_object_set(ret, "args", list_to_json(expr->args));
  json_object_set(ret, "location", json_integer(expr->location));
  return ret;
}

static json_t *op_expr_to_json(const OpExpr *expr) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("OpExpr"));
  json_object_set(ret, "opno", oid_to_json(expr->opno));
  json_object_set(ret, "opfuncid", oid_to_json(expr->opfuncid));
  json_object_set(ret, "opresulttype", oid_to_json(expr->opresulttype));
  json_object_set(ret, "opretset", json_boolean(expr->opretset));
  json_object_set(ret, "opcollid", oid_to_json(expr->opcollid));
  json_object_set(ret, "inputcollid", oid_to_json(expr->inputcollid));
  json_object_set(ret, "args", list_to_json(expr->args));
  json_object_set(ret, "location", json_integer(expr->location));
  return ret;
}

static json_t *var_to_json(const Var *var) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("Var"));
  json_object_set(ret, "varno", json_integer(var->varno));
  json_object_set(ret, "varattrno", json_integer(var->varattno));
  json_object_set(ret, "vartype", oid_to_json(var->vartype));
  json_object_set(ret, "vartypemode", json_integer(var->vartypmod));
  json_object_set(ret, "varcollid", oid_to_json(var->varcollid));
  json_object_set(ret, "levelsup", json_integer(var->varlevelsup));
  json_object_set(ret, "syntactic relation index",
                  json_integer(var->varattnosyn));
  json_object_set(ret, "location", json_integer(var->location));

  return ret;
}

static json_t *const_to_json(const Const *c) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("Const"));
  json_object_set(ret, "consttype", oid_to_json(c->consttype));
  json_object_set(ret, "consttypemod", json_integer(c->consttypmod));
  json_object_set(ret, "collid", json_integer(c->constcollid));
  json_object_set(ret, "len", json_integer(c->constlen));
  // value
  json_object_set(ret, "isNull", json_boolean(c->constisnull));
  json_object_set(ret, "passbyval", json_boolean(c->constbyval));
  json_object_set(ret, "location", json_integer(c->location));
  return ret;
}

static json_t *te_to_json(const TargetEntry *te) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("TargetEntry"));
  json_object_set(ret, "expr", node_to_json((Node *)te->expr));
  json_object_set(ret, "resno", json_integer(te->resno));
  json_object_set(ret, "resname", json_string(te->resname));
  json_object_set(ret, "ref by sort/group", json_integer(te->ressortgroupref));
  json_object_set(ret, "col oid in orignal tb", oid_to_json(te->resorigtbl));
  json_object_set(ret, "colno in source tb", json_integer(te->resorigcol));
  json_object_set(ret, "junk", json_boolean(te->resjunk));
  return ret;
}

static json_t *sort_group_clause_to_json(const SortGroupClause *clause) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("SortGroupClause"));
  json_object_set(ret, "ref to tl", json_integer(clause->tleSortGroupRef));
  json_object_set(ret, "eq op", oid_to_json(clause->eqop));
  json_object_set(ret, "sort op", oid_to_json(clause->sortop));
  json_object_set(ret, "nulls_first", json_boolean(clause->nulls_first));
  json_object_set(ret, "hashable", json_boolean(clause->hashable));
  return ret;
}

static json_t *pointer_array_to_json(Node **node_arr, int size) {
  json_t *ret = json_array();
  for (int i = 0; i < size; ++i) {
    json_array_append(ret, node_to_json(node_arr[i]));
  }
  return ret;
}

static json_t *planner_info_to_json(const PlannerInfo *root) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("PlannerInfo"));
  json_object_set(ret, "query", query_to_json(root->parse));
  json_object_set(ret, "planner_global", node_to_json((Node *)root->glob));
  json_object_set(ret, "level", json_integer(root->query_level));
  json_object_set(ret, "parent", node_to_json((Node *)root->parent_root));
  json_object_set(ret, "plan params", list_to_json(root->plan_params));
  json_object_set(ret, "outer params", bitmapset_to_json(root->outer_params));
  json_object_set(ret, "simple rel arr size",
                  json_integer(root->simple_rel_array_size));
  json_object_set(ret, "simple rels",
                  pointer_array_to_json((Node **)root->simple_rel_array,
                                        root->simple_rel_array_size));
  json_object_set(ret, "processed tlist", list_to_json(root->processed_tlist));
  return ret;
}

static json_t *planner_global_to_json(const PlannerGlobal *planner_global) {
  json_t *ret = json_pack("{}");
  return ret;
}

static json_t *rel_opt_info_to_json(const RelOptInfo *rel_opt_info) {
  json_t *ret = json_pack("{}");
  return ret;
}

static json_t *win_func_to_json(const WindowFunc* wf) {
  json_t *ret = json_pack("{}");
  json_object_set(ret, "type", json_string("WindowFunc"));
  json_object_set(ret, "winfnoid", oid_to_json(wf->winfnoid));
  json_object_set(ret, "wintype", oid_to_json(wf->wintype));
  json_object_set(ret, "win_coll_id", oid_to_json(wf->wincollid));
  json_object_set(ret, "input_coll_id", oid_to_json(wf->inputcollid));
  json_object_set(ret, "args", list_to_json(wf->args));
  json_object_set(ret, "agg_filter", node_to_json((Node*)wf->aggfilter));
  json_object_set(ret, "win_ref", json_integer(wf->winref));
  json_object_set(ret, "winstar", json_boolean(wf->winstar));
  json_object_set(ret, "winagg", json_boolean(wf->winagg));
  json_object_set(ret, "location", json_integer(wf->location));
  return ret;
}

static json_t *win_clause_to_json(const WindowClause* wc) {
	json_t *ret = json_pack("{}");
	json_object_set(ret, "type", json_string("WindowClause"));
	json_object_set(ret, "name", json_string(wc->name));
	json_object_set(ret, "refname", json_string(wc->refname));
	json_object_set(ret, "partition list", list_to_json(wc->partitionClause));
	json_object_set(ret, "order list", list_to_json(wc->orderClause));
	json_object_set(ret, "frame opt", json_integer(wc->frameOptions));
	json_object_set(ret, "start offset", node_to_json(wc->startOffset));
	json_object_set(ret, "end offset", node_to_json(wc->endOffset));
	json_object_set(ret, "run cond", list_to_json(wc->runCondition));
	json_object_set(ret, "start in range func", oid_to_json(wc->startInRangeFunc));
	json_object_set(ret, "end in range func", oid_to_json(wc->endInRangeFunc));
	json_object_set(ret, "in range coll", oid_to_json(wc->inRangeColl));
	json_object_set(ret, "in range asc", json_boolean(wc->inRangeAsc));
	json_object_set(ret, "in range nulls first", json_boolean(wc->inRangeNullsFirst));
	json_object_set(ret, "win ref", json_integer(wc->winref));
	json_object_set(ret, "copied order", json_boolean(wc->copiedOrder));
	return ret;
}

static json_t *node_to_json(const Node *n) {
  json_t *ret;
  if (!n)
    return json_null();
  switch (n->type) {
  case T_Alias: {
    Alias *alias = (Alias *)n;
    ret = json_pack("{}");
    json_object_set(ret, "type", json_string("Alias"));
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
  case T_FromExpr: {
    FromExpr *expr = (FromExpr *)n;
    ret = from_expr_to_json(expr);
    break;
  }
  case T_RangeTblRef: {
    RangeTblRef *rtr = (RangeTblRef *)n;
    ret = rtr_to_json(rtr);
    break;
  }
  case T_JoinExpr: {
    JoinExpr *expr = (JoinExpr *)n;
    ret = join_expr_to_json(expr);
    break;
  }
  case T_BoolExpr: {
    BoolExpr *expr = (BoolExpr *)n;
    ret = bool_expr_to_json(expr);
    break;
  }
  case T_OpExpr: {
    OpExpr *expr = (OpExpr *)n;
    ret = op_expr_to_json(expr);
    break;
  }
  case T_Var: {
    Var *var = (Var *)n;
    ret = var_to_json(var);
    break;
  }
  case T_Const: {
    Const *c = (Const *)n;
    ret = const_to_json(c);
    break;
  }
  case T_TargetEntry: {
    TargetEntry *te = (TargetEntry *)n;
    ret = te_to_json(te);
    break;
  }
  case T_SortGroupClause: {
    SortGroupClause *clause = (SortGroupClause *)n;
    ret = sort_group_clause_to_json(clause);
    break;
  }
  case T_PlannerInfo: {
    PlannerInfo *root = (PlannerInfo *)n;
    ret = planner_info_to_json(root);
    break;
  }
  case T_PlannerGlobal: {
    PlannerGlobal *planner_global = (PlannerGlobal *)n;
    ret = planner_global_to_json(planner_global);
    break;
  }
  case T_RelOptInfo: {
    RelOptInfo *nn = (RelOptInfo *)n;
    ret = rel_opt_info_to_json(nn);
    break;
  }
  case T_WindowFunc: {
    WindowFunc *wf = (WindowFunc *)n;
    ret = win_func_to_json(wf);
    break;
  }
  case T_WindowClause: {
    WindowClause *wc = (WindowClause *)n;
    ret = win_clause_to_json(wc);
    break;
  }
  default:
    ret = json_pack("[si]", "not implemented type: ", n->type);
    break;
  }
  return ret;
}

static json_t *query_to_json(const Query *query) {
  ListCell *lc;
  json_t *json = json_pack("{}");
  json_t* rtable_list;

  if (!query) return json;
  rtable_list = json_array();

  json_object_set(json, "query_id", json_integer(query->queryId));
  json_object_set(json, "can_set_tag", json_boolean(query->canSetTag));
  json_object_set(json, "has_agg", json_boolean(query->hasAggs));
  json_object_set(json, "has_window_func", json_boolean(query->hasWindowFuncs));
  json_object_set(json, "has_set_returning_funcs",
                  json_boolean(query->hasTargetSRFs));
  json_object_set(json, "has_sublinks", json_boolean(query->hasSubLinks));
  json_object_set(json, "has distinct on", json_boolean(query->hasDistinctOn));
  json_object_set(json, "has for update", json_boolean(query->hasForUpdate));
  json_object_set(json, "has row security",
                  json_boolean(query->hasRowSecurity));

  json_object_set(json, "rtable", rtable_list);
  foreach (lc, query->rtable) {
    json_array_append(rtable_list, node_to_json(lfirst_node(Node, lc)));
  }
  json_object_set(json, "jointree", node_to_json((Node *)query->jointree));
  json_object_set(json, "mergeActionList",
                  list_to_json(query->mergeActionList));
  json_object_set(json, "targetlist", list_to_json(query->targetList));
  json_object_set(json, "returning list", list_to_json(query->returningList));
  json_object_set(json, "group", list_to_json(query->groupClause));
  json_object_set(json, "groupdistinct", json_boolean(query->groupDistinct));
  json_object_set(json, "groupingsets", list_to_json(query->groupingSets));
  json_object_set(json, "havingqual", node_to_json(query->havingQual));
  json_object_set(json, "windowClause", list_to_json(query->windowClause));
  json_object_set(json, "distinctClause", list_to_json(query->distinctClause));
  json_object_set(json, "sortClause", list_to_json(query->sortClause));
  json_object_set(json, "limitOffset", node_to_json(query->limitOffset));
  json_object_set(json, "limitCount", node_to_json(query->limitCount));
  json_object_set(json, "rowmarks", list_to_json(query->rowMarks));
  json_object_set(json, "setOperations", node_to_json(query->setOperations));
  return json;
}

void print_query(const Query *query) {
  json_t *json = query_to_json(query);
  // elog(INFO, "%s", json_dumps(json, JSON_INDENT(2)));
  json_dump_file(json, "/tmp/query.json", JSON_INDENT(2));
  json_decref(json);
}

void print_planner_info(const PlannerInfo *root) {
  json_t *json = node_to_json((Node*)root);
  json_dump_file(json, "/tmp/planner_info.json", JSON_INDENT(2));
  json_decref(json);
}
