#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "bloom.h"

PG_FUNCTION_INFO_V1(blbeginscan);
Datum       blbeginscan(PG_FUNCTION_ARGS);
Datum
blbeginscan(PG_FUNCTION_ARGS)
{
    Relation    rel = (Relation) PG_GETARG_POINTER(0);
	int         keysz = PG_GETARG_INT32(1);
	ScanKey     scankey = (ScanKey) PG_GETARG_POINTER(2);
	IndexScanDesc scan;

	scan = RelationGetIndexScan(rel, keysz, scankey);

	PG_RETURN_POINTER(scan);
}

PG_FUNCTION_INFO_V1(blrescan);
Datum       blrescan(PG_FUNCTION_ARGS);
Datum
blrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey     scankey = (ScanKey) PG_GETARG_POINTER(1);
	BloomScanOpaque so;

	so = (BloomScanOpaque) scan->opaque;

	if (so == NULL)
	{
		/* if called from blbeginscan */
		so = (BloomScanOpaque) palloc(sizeof(BloomScanOpaqueData));
		initBloomState(&so->state, scan->indexRelation);
		scan->opaque = so;

	}
	else
	{
		if (so->sign)
			pfree(so->sign);
	}
	so->sign = NULL;

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

	}

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(blendscan);
Datum       blendscan(PG_FUNCTION_ARGS);
Datum
blendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	BloomScanOpaque so = (BloomScanOpaque) scan->opaque;

	if (so->sign)
		pfree(so->sign);
	so->sign = NULL;

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(blmarkpos);
Datum       blmarkpos(PG_FUNCTION_ARGS);
Datum
blmarkpos(PG_FUNCTION_ARGS)
{
    elog(ERROR, "Bloom does not support mark/restore");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(blrestrpos);
Datum       blrestrpos(PG_FUNCTION_ARGS);
Datum
blrestrpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Bloom does not support mark/restore");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(blgetbitmap);
Datum       blgetbitmap(PG_FUNCTION_ARGS);
Datum
blgetbitmap(PG_FUNCTION_ARGS)
{
	IndexScanDesc 			scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  				*tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	int64					ntids = 0;
	BlockNumber				blkno = BLOOM_HEAD_BLKNO,
							npages;
	int						i;
	BufferAccessStrategy	bas;
	BloomScanOpaque 		so = (BloomScanOpaque) scan->opaque;

	PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno);

	if (so->sign == NULL && scan->numberOfKeys > 0)
	{
		/* new search without full scan */
		ScanKey skey = scan->keyData;	

		so->sign = palloc0( sizeof(SignType) * so->state.opts->bloomLength ); 
		
		for(i=0; i<scan->numberOfKeys;i++)
		{
			/*
			 * Assume, that Bloom-indexable operators are strict, so nothing could
		     * be found
		     */

			if (skey->sk_flags & SK_ISNULL)
			{
				pfree(so->sign);
				so->sign = NULL;
				return 0;
			}

			signValue(&so->state, so->sign, skey->sk_argument, skey->sk_attno - 1);

			skey++;
		}
	}

	bas = GetAccessStrategy(BAS_BULKREAD);

    if (!RELATION_IS_LOCAL(scan->indexRelation))
		LockRelationForExtension(scan->indexRelation, ShareLock);
	npages = RelationGetNumberOfBlocks(scan->indexRelation);
    if (!RELATION_IS_LOCAL(scan->indexRelation))
		UnlockRelationForExtension(scan->indexRelation, ShareLock);

	for(blkno=BLOOM_HEAD_BLKNO; blkno < npages; blkno++)
	{
		Buffer 			buffer;
		Page			page;

		buffer = ReadBufferExtended(
						scan->indexRelation, MAIN_FORKNUM,
						blkno, RBM_NORMAL, bas);
		if (blkno+1 < npages)
			PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno + 1);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (!BloomPageIsDeleted(page))
		{
			BloomTuple	*itup = BloomPageGetData(page);
			BloomTuple   *itupEnd = (BloomTuple*)( ((char*)itup) + 
								so->state.sizeOfBloomTuple * BloomPageGetMaxOffset(page));

			while(itup < itupEnd)
			{
				bool res = true;

				for(i=0; res && i<so->state.opts->bloomLength; i++)
					if ( (itup->sign[i] & so->sign[i]) != so->sign[i] )
						res = false;
	
				if (res)
				{
					tbm_add_tuples(tbm, &itup->heapPtr, 1, true);
					ntids++;
				}

				itup = (BloomTuple*)( ((char*)itup) + so->state.sizeOfBloomTuple );
			}
		}

		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}
	FreeAccessStrategy(bas);

	PG_RETURN_INT64(ntids);
}

