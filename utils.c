#include "utils.h"

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

const char *get_rel_kind_str(char rel_kind) {
  if (rel_kind < 'A' || rel_kind > 'z')
    return "";
  return rel_kind_map[rel_kind - 'A' + 1];
}


const char* get_rte_kind_str(RTEKind kind) {
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
