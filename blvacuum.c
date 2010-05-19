#include "postgres.h"

#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "bloom.h"

PG_FUNCTION_INFO_V1(blbulkdelete);
Datum       blbulkdelete(PG_FUNCTION_ARGS);
Datum
blbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo 		*info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult 	*stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void       				*callback_state = (void *) PG_GETARG_POINTER(3);
	Relation    			index = info->index;
	BlockNumber             blkno,
							npages;
	FreeBlockNumberArray	notFullPage;
	int						countPage = 0;
	BloomState				state;
	bool					needLock;
	Buffer					buffer;
    Page            		page;


	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	initBloomState(&state, index); 

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	for(blkno=BLOOM_HEAD_BLKNO; blkno<npages; blkno++)
	{
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

        LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (!BloomPageIsDeleted(page))
		{
        	BloomTuple	*itup = BloomPageGetData(page);
			BloomTuple	*itupEnd = (BloomTuple*)( ((char*)itup) + 
								 state.sizeOfBloomTuple * BloomPageGetMaxOffset(page));
			BloomTuple	*itupPtr = itup;

			while(itup < itupEnd)
			{
				if (callback(&itup->heapPtr, callback_state))
				{
					stats->tuples_removed += 1;
					START_CRIT_SECTION();
					BloomPageGetOpaque(page)->maxoff--;
					END_CRIT_SECTION();
				} 
				else 
				{
					if ( itupPtr != itup )
					{
						START_CRIT_SECTION();
						memcpy(itupPtr, itup, state.sizeOfBloomTuple);
						END_CRIT_SECTION();
					}
					stats->num_index_tuples++;
					itupPtr = (BloomTuple*)( ((char*)itupPtr) + state.sizeOfBloomTuple );
				}

				itup = (BloomTuple*)( ((char*)itup) + state.sizeOfBloomTuple );
			}

			if (itupPtr != itup)
			{
				if (itupPtr == BloomPageGetData(page))
				{
					START_CRIT_SECTION();
					BloomPageSetDeleted(page);
					END_CRIT_SECTION();
				}
				MarkBufferDirty(buffer);
			}

			if (!BloomPageIsDeleted(page) && 
						BloomPageGetFreeSpace(&state, page) > state.sizeOfBloomTuple && 
						countPage < BloomMetaBlockN)
				notFullPage[countPage++] = blkno;
		}

        UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

	if (countPage>0) 
	{
		BloomMetaPageData	*metaData;

		buffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		metaData = BloomPageGetMeta(page);
		START_CRIT_SECTION();
		memcpy(metaData->notFullPage, notFullPage, sizeof(FreeBlockNumberArray));
		metaData->nStart=0;
		metaData->nEnd = countPage;
		END_CRIT_SECTION();

		MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
	}

	PG_RETURN_POINTER(stats);
}

PG_FUNCTION_INFO_V1(blvacuumcleanup);
Datum       blvacuumcleanup(PG_FUNCTION_ARGS);
Datum
blvacuumcleanup(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation    index = info->index;
	bool        needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	BlockNumber lastBlock = BLOOM_HEAD_BLKNO,
				lastFilledBlock = BLOOM_HEAD_BLKNO;

	if (info->analyze_only)
		PG_RETURN_POINTER(stats);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	totFreePages = 0;
	for (blkno = BLOOM_HEAD_BLKNO; blkno < npages; blkno++)
	{
		Buffer      buffer;
		Page        page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = (Page) BufferGetPage(buffer);
																						
		if (BloomPageIsDeleted(page))
		{
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else
		{
			lastFilledBlock = blkno;
			stats->num_index_tuples += BloomPageGetMaxOffset(page);
			stats->estimated_count += BloomPageGetMaxOffset(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	lastBlock = npages - 1;
	if (lastBlock > lastFilledBlock)
	{
		RelationTruncate(index, lastFilledBlock + 1);
		stats->pages_removed = lastBlock - lastFilledBlock;
		totFreePages = totFreePages - stats->pages_removed;
	}

	IndexFreeSpaceMapVacuum(info->index);
	stats->pages_free = totFreePages;

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	PG_RETURN_POINTER(stats);
}
