#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "bloom.h"

PG_MODULE_MAGIC;

typedef struct
{
	BloomState		blstate;
	MemoryContext	tmpCtx;
	Buffer			currentBuffer;
	Page			currentPage;
} BloomBuildState;

static void
bloomBuildCallback(Relation index, HeapTuple htup, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	BloomBuildState	*buildstate = (BloomBuildState*)state;
	MemoryContext	oldCtx;
	BloomTuple		*itup;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	itup = BloomFormTuple(&buildstate->blstate, &htup->t_self, values, isnull);

	if (buildstate->currentBuffer == InvalidBuffer ||
			BloomPageAddItem(&buildstate->blstate, buildstate->currentPage, itup) == false) 
	{
		if (buildstate->currentBuffer != InvalidBuffer)
		{
			MarkBufferDirty(buildstate->currentBuffer);
			UnlockReleaseBuffer(buildstate->currentBuffer);
		}

		CHECK_FOR_INTERRUPTS();

		/* BloomNewBuffer returns locked page */
		buildstate->currentBuffer = BloomNewBuffer(index);
		BloomInitBuffer(buildstate->currentBuffer, 0);
		buildstate->currentPage = BufferGetPage(buildstate->currentBuffer);
		
		if (BloomPageAddItem(&buildstate->blstate, buildstate->currentPage, itup) == false)
			elog(ERROR, "can not add new tuple"); /* should not be here! */
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

PG_FUNCTION_INFO_V1(blbuild);
Datum       blbuild(PG_FUNCTION_ARGS);
Datum
blbuild(PG_FUNCTION_ARGS)
{   
    Relation    heap = (Relation) PG_GETARG_POINTER(0);
	Relation    index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double      reltuples;
	BloomBuildState buildstate;
	Buffer		MetaBuffer;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			RelationGetRelationName(index));

	/* initialize the meta page */
	MetaBuffer = BloomNewBuffer(index);

	START_CRIT_SECTION();
	BloomInitMetabuffer(MetaBuffer, index);
	MarkBufferDirty(MetaBuffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(MetaBuffer);

	initBloomState(&buildstate.blstate, index);

	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
												"Bloom build temporary context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.currentBuffer = InvalidBuffer;

	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
									bloomBuildCallback, (void *) &buildstate);

	/* close opened buffer */
	if (buildstate.currentBuffer != InvalidBuffer)
	{
		MarkBufferDirty(buildstate.currentBuffer);
		UnlockReleaseBuffer(buildstate.currentBuffer);
	}

	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = reltuples;

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(blbuildempty);
Datum       blbuildempty(PG_FUNCTION_ARGS);
Datum
blbuildempty(PG_FUNCTION_ARGS)
{
	elog(NOTICE, "blbuildempty: FIX ME");
        ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("unlogged bloom indexes are not supported")));

        PG_RETURN_VOID();
}

static bool
addItemToBlock(Relation index, BloomState *state, BloomTuple *itup, BlockNumber blkno)
{
	Buffer		buffer;
	Page		page;

	buffer = ReadBuffer(index, blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	START_CRIT_SECTION();
	if (BloomPageAddItem(state, page, itup))
	{
		/* inserted */
		END_CRIT_SECTION();
		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);
		return true;
	}
	else
	{
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
		return false;
	}
}

PG_FUNCTION_INFO_V1(blinsert);
Datum       blinsert(PG_FUNCTION_ARGS);
Datum
blinsert(PG_FUNCTION_ARGS)
{
	Relation    index = (Relation) PG_GETARG_POINTER(0);
	Datum      *values = (Datum *) PG_GETARG_POINTER(1);
	bool       *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer) PG_GETARG_POINTER(3);
#ifdef NOT_USED
    Relation    heapRel = (Relation) PG_GETARG_POINTER(4);
	IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
#endif
	BloomState   	 blstate;
	BloomTuple		*itup;
	MemoryContext 	oldCtx;
	MemoryContext 	insertCtx;
	BloomMetaPageData	*metaData;
	Buffer				metaBuffer,
						buffer;
	BlockNumber			blkno = InvalidBlockNumber;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
										"Bloom insert temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initBloomState(&blstate, index);
	itup = BloomFormTuple(&blstate, ht_ctid, values, isnull);

	metaBuffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
	metaData = BloomPageGetMeta(BufferGetPage(metaBuffer));

	if (metaData->nEnd > metaData->nStart)
	{
		blkno = metaData->notFullPage[ metaData->nStart ];

		Assert(blkno != InvalidBlockNumber);
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

		if (addItemToBlock(index, &blstate, itup, blkno))
			goto away;
	}
	else
	{
		/* no avaliable pages */
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
	}

	/* protect any changes on metapage with a help of CRIT_SECTION */

	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if ( metaData->nEnd > metaData->nStart && 
		blkno == metaData->notFullPage[ metaData->nStart ] )
		metaData->nStart++;
	END_CRIT_SECTION();
			
	while( metaData->nEnd > metaData->nStart )
	{
		blkno = metaData->notFullPage[ metaData->nStart ];

		Assert(blkno != InvalidBlockNumber);
		if (addItemToBlock(index, &blstate, itup, blkno))
		{
			MarkBufferDirty(metaBuffer);
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			goto away;
		}

		START_CRIT_SECTION();
		metaData->nStart++;
		END_CRIT_SECTION();
	}

	/* no free pages */
	buffer = BloomNewBuffer(index);
	BloomInitBuffer(buffer, 0);
	BloomPageAddItem(&blstate, BufferGetPage(buffer), itup);

	START_CRIT_SECTION();
	metaData->nStart = 0;
	metaData->nEnd = 1;
	metaData->notFullPage[ 0 ] = BufferGetBlockNumber(buffer);
	END_CRIT_SECTION();

	MarkBufferDirty(metaBuffer);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

away:
	ReleaseBuffer(metaBuffer);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	PG_RETURN_BOOL(false);
}
