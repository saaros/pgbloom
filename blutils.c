#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"

#include "bloom.h"


void 
initBloomState(BloomState *state, Relation index)
{
	int	i;

	state->nColumns = index->rd_att->natts;

	for (i = 0; i < index->rd_att->natts; i++)
	{
		fmgr_info_copy(&(state->hashFn[i]),
						index_getprocinfo(index, i + 1, BLOOM_HASH_PROC),
						CurrentMemoryContext);
	}

	if (!index->rd_amcache)
	{
		Buffer				buffer;
		BloomMetaPageData	*meta;
		BloomOptions		*opts;

		opts = MemoryContextAlloc(index->rd_indexcxt, sizeof(BloomOptions));

		buffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		if (!BloomPageIsMeta(BufferGetPage(buffer)))
			elog(ERROR,"Relation is not a bloom index");
		meta = BloomPageGetMeta(BufferGetPage(buffer));

		if (meta->magickNumber != BLOOM_MAGICK_NUMBER)
			elog(ERROR,"Relation is not a bloom index");

		*opts = meta->opts;

		UnlockReleaseBuffer(buffer);

		index->rd_amcache = (void*)opts;
	}

	state->opts = (BloomOptions*)index->rd_amcache;
	state->sizeOfBloomTuple = BLOOMTUPLEHDRSZ + sizeof(SignType) * state->opts->bloomLength; 
}

void
signValue(BloomState *state, SignType *sign, Datum value, int attno)
{
	uint32		hashVal;
	int 		nBit, j;

	/*
	 * init generator with "column's" number to get
	 * "hashed" seed for new value. We don't want to map
	 * the same numbers from different columns into the same bits!
	 */
	srand(attno);

	/*
	* Init hash sequence to map our value into bits. the same values
	* in different columns will be mapped into different bits because
	* of step above
	*/
	hashVal = DatumGetInt32(FunctionCall1(
								&state->hashFn[attno],
								value
			 	));
	srand(hashVal ^ rand());

	for(j=0; j<state->opts->bitSize[attno]; j++)
	{
		/* prevent mutiple evaluation */
		nBit = rand() % (state->opts->bloomLength * BITSIGNTYPE); 
		SETBIT(sign, nBit);
	}
}

BloomTuple*
BloomFormTuple(BloomState *state, ItemPointer iptr, Datum *values, bool *isnull)
{
	int 		i;
	BloomTuple	*res = palloc0(state->sizeOfBloomTuple);

	res->heapPtr = *iptr;

    /*
	 * Blooming
	 */
	for(i=0; i<state->nColumns; i++)
	{
		/*
		 * skip nulls
		 */
		if ( isnull[i] )
			continue;

		signValue(state, res->sign, values[i], i);

	}

	return res;
}

bool
BloomPageAddItem(BloomState *state, Page p, BloomTuple *t)
{
	BloomTuple		*pagePtr;
	BloomPageOpaque	opaque;

	if (BloomPageGetFreeSpace(state, p) < state->sizeOfBloomTuple)
		return false;

	opaque = BloomPageGetOpaque(p);
	pagePtr = BloomPageGetData(p);
	memcpy(((char*)pagePtr) + opaque->maxoff * state->sizeOfBloomTuple, 
				t, state->sizeOfBloomTuple);
	opaque->maxoff++;

	return true;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling BloomInitBuffer
 */

Buffer
BloomNewBuffer(Relation index)
{
	Buffer      buffer;
	bool        needLock;

	/* First, try to get a page from FSM */
	for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);

		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer))
		{
			Page        page = BufferGetPage(buffer);

			if (PageIsNew(page))
				return buffer;  /* OK to use, if never initialized */

			if (BloomPageIsDeleted(page))
				return buffer;  /* OK to use */

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* Must extend the file */
	needLock = !RELATION_IS_LOCAL(index);
	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	return buffer;
}

void
BloomInitBuffer(Buffer b, uint16 f)
{
    BloomInitPage(BufferGetPage(b), f, BufferGetPageSize(b));
}

void
BloomInitPage(Page page, uint16 f, Size pageSize)
{
    BloomPageOpaque opaque;
	 
	PageInit(page, pageSize, sizeof(BloomPageOpaqueData));
		  
	opaque = BloomPageGetOpaque(page);
	memset(opaque, 0, sizeof(BloomPageOpaqueData));
	opaque->maxoff = 0;
	opaque->flags = f;
}


static BloomOptions*
makeDefaultBloomOptions(BloomOptions *opts)
{
	int i;

	if (!opts)
		opts = palloc0(sizeof(BloomOptions));

	if (opts->bloomLength <=0)
		opts->bloomLength = 5;

	for(i=0;i<INDEX_MAX_KEYS;i++)
		if (opts->bitSize[i] <= 0 || opts->bitSize[i] >= opts->bloomLength * sizeof(SignType))
			opts->bitSize[i] = 2;

	return opts;
}

void
BloomInitMetabuffer(Buffer b, Relation index)
{
	BloomMetaPageData	*metadata;
	Page				page = BufferGetPage(b);

	BloomInitPage(page, BLOOM_META, BufferGetPageSize(b));
	metadata = BloomPageGetMeta(page);
	memset(metadata, 0, sizeof(BloomMetaPageData));
	metadata->magickNumber = BLOOM_MAGICK_NUMBER;
	metadata->opts = *makeDefaultBloomOptions((BloomOptions*)index->rd_options);
}

static relopt_kind bloom_kind = 0;

void _PG_init(void);
void 
_PG_init(void)
{
	int i;
	char				buf[16];

	bloom_kind = add_reloption_kind();

	add_int_reloption(bloom_kind, "length", "Length of signature in uint16 type",
						5, 1, 256);

	for(i=0;i<INDEX_MAX_KEYS;i++)
	{
		snprintf(buf, 16, "col%d", i+1);
		add_int_reloption(bloom_kind, buf, "Number of bits for corresponding column",
								2, 1, 2048);
	}
}

PG_FUNCTION_INFO_V1(bloptions);
Datum       bloptions(PG_FUNCTION_ARGS);
Datum
bloptions(PG_FUNCTION_ARGS)
{
    Datum       		reloptions = PG_GETARG_DATUM(0);
	bool        		validate = PG_GETARG_BOOL(1);
	relopt_value 		*options;
	int					numoptions;
	BloomOptions		*rdopts;
	relopt_parse_elt 	tab[INDEX_MAX_KEYS+1];
	int 				i;
	char				buf[16];

	tab[0].optname = "length";
	tab[0].opttype = RELOPT_TYPE_INT;
	tab[0].offset = offsetof(BloomOptions, bloomLength);

	for(i=0;i<INDEX_MAX_KEYS;i++)
	{
		snprintf(buf, sizeof(buf), "col%d", i+1);
		tab[i+1].optname = pstrdup(buf);
		tab[i+1].opttype = RELOPT_TYPE_INT;
		tab[i+1].offset = offsetof(BloomOptions, bitSize[i]);
	}

	options = parseRelOptions(reloptions, validate, bloom_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(BloomOptions), options, numoptions);
	fillRelOptions((void *) rdopts, sizeof(BloomOptions), options, numoptions,
						validate, tab, INDEX_MAX_KEYS+1);
		
	rdopts = makeDefaultBloomOptions(rdopts);

	PG_RETURN_BYTEA_P(rdopts);
}
