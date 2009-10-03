#include "postgres.h"

#include "fmgr.h"
#include "optimizer/cost.h"
#include "utils/selfuncs.h"

#include "bloom.h"

PG_FUNCTION_INFO_V1(blcostestimate);
Datum       blcostestimate(PG_FUNCTION_ARGS);
Datum
blcostestimate(PG_FUNCTION_ARGS)
{
	/* PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0); */
	IndexOptInfo *index = (IndexOptInfo *) PG_GETARG_POINTER(1);
	/* List       *indexQuals = (List *) PG_GETARG_POINTER(2); */
	RelOptInfo *outer_rel = (RelOptInfo *) PG_GETARG_POINTER(3);
	Cost       *indexStartupCost = (Cost *) PG_GETARG_POINTER(4);
	Cost       *indexTotalCost = (Cost *) PG_GETARG_POINTER(5);
	/* Selectivity *indexSelectivity = (Selectivity *) PG_GETARG_POINTER(6); */
	/* double     *indexCorrelation = (double *) PG_GETARG_POINTER(7); */

	/* we believe that gistcostestimate just call generic cost estimate */
	DirectFunctionCall8(
		gistcostestimate,
		PG_GETARG_DATUM(0),
		PG_GETARG_DATUM(1),
		PG_GETARG_DATUM(2),
		PG_GETARG_DATUM(3),
		PG_GETARG_DATUM(4),
		PG_GETARG_DATUM(5),
		PG_GETARG_DATUM(6),
		PG_GETARG_DATUM(7)
	);

	*indexStartupCost = 0;
	*indexTotalCost = index->pages * seq_page_cost / 2 /* cache effects */;
	if (outer_rel != NULL && outer_rel->rows > 1)
		*indexTotalCost  *= outer_rel->rows;

	PG_RETURN_VOID();
}
