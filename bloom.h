#ifndef _BLOOM_H_
#define _BLOOM_H_

#include "access/genam.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"

#define	BLOOM_HASH_PROC		1
#define	BLLOMNProc			1

typedef struct BloomPageOpaqueData
{
	OffsetNumber	maxoff;
	uint16			flags;
} BloomPageOpaqueData;

typedef BloomPageOpaqueData *BloomPageOpaque;

#define BLOOM_META		(1<<0)
#define BLOOM_DELETED	(2<<0)

#define BloomPageGetOpaque(page) ( (BloomPageOpaque) PageGetSpecialPointer(page) )
#define BloomPageGetMaxOffset(page) ( BloomPageGetOpaque(page)->maxoff )
#define BloomPageIsMeta(page) ( BloomPageGetOpaque(page)->flags & BLOOM_META)
#define BloomPageIsDeleted(page) ( BloomPageGetOpaque(page)->flags & BLOOM_DELETED)
#define BloomPageSetDeleted(page)    ( BloomPageGetOpaque(page)->flags |= BLOOM_DELETED)
#define BloomPageSetNonDeleted(page) ( BloomPageGetOpaque(page)->flags &= ~BLOOM_DELETED)
#define BloomPageGetData(page)		(  (BloomTuple*)PageGetContents(page) )

#define BLOOM_METAPAGE_BLKNO  	(0)
#define BLOOM_HEAD_BLKNO  		(1)

typedef struct BloomOptions 
{
	int32       vl_len_;	/* varlena header (do not touch directly!) */
	int		bloomLength;	
	int		bitSize[INDEX_MAX_KEYS];
} BloomOptions;



typedef BlockNumber FreeBlockNumberArray[
			MAXALIGN_DOWN(
				BLCKSZ - 
					SizeOfPageHeaderData - 
					MAXALIGN(sizeof(BloomPageOpaqueData)) - 
					/* header of BloomMetaPageData struct */
					MAXALIGN(sizeof(uint16) * 2 + sizeof(uint32) + sizeof(BloomOptions)) 
			) / sizeof(BlockNumber) 
		];

typedef struct BloomMetaPageData
{
	uint32					magickNumber;
	uint16					nStart;
	uint16					nEnd;
	BloomOptions			opts;
	FreeBlockNumberArray	notFullPage;
} BloomMetaPageData;

#define BLOOM_MAGICK_NUMBER	(0xDBAC0DED)

#define BloomMetaBlockN		(sizeof(FreeBlockNumberArray) / sizeof(BlockNumber))
#define BloomPageGetMeta(p) \
	((BloomMetaPageData *) PageGetContents(p))

typedef struct BloomState 
{
	FmgrInfo			hashFn[INDEX_MAX_KEYS];
	BloomOptions		*opts; /* stored in rd_amcache and defined at creation time */
	int32				nColumns;
	/* 
	 * sizeOfBloomTuple is index's specific, and it depends on
	 * reloptions, so precompute it
	 */
	int32				sizeOfBloomTuple; 
} BloomState;

#define BloomPageGetFreeSpace(state, page) \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
		- BloomPageGetMaxOffset(page) * (state)->sizeOfBloomTuple \
		- MAXALIGN(sizeof(BloomPageOpaqueData)))

/*
 * Tuples are very different from all other relations
 */
typedef uint16	SignType;

typedef struct BloomTuple {
	ItemPointerData		heapPtr;
	SignType			sign[1];
} BloomTuple;
#define BLOOMTUPLEHDRSZ	offsetof(BloomTuple, sign)

#define BITBYTE 	(8)
#define BITSIGNTYPE	(BITBYTE * sizeof(SignType))
#define GETWORD(x,i) ( *( (SignType*)(x) + (int)( (i) / BITSIGNTYPE ) ) )
#define GETBITBYTE(x,i) ( ((SignType)(x)) >> i & 0x01 )
#define CLRBIT(x,i)   GETWORD(x,i) &= ~( 0x01 << ( (i) % BITSIGNTYPE ) )
#define SETBIT(x,i)   GETWORD(x,i) |=  ( 0x01 << ( (i) % BITSIGNTYPE ) )
#define GETBIT(x,i) ( (GETWORD(x,i) >> ( (i) % BITSIGNTYPE )) & 0x01 )

typedef struct BloomScanOpaqueData
{
	SignType	*sign;
	BloomState	state;
} BloomScanOpaqueData;

typedef BloomScanOpaqueData *BloomScanOpaque;

/* blutils.c */
extern void initBloomState(BloomState *state, Relation index);
extern void BloomInitMetabuffer(Buffer b, Relation index);
extern void BloomInitBuffer(Buffer b, uint16 f);
extern void BloomInitPage(Page page, uint16 f, Size pageSize);
extern Buffer BloomNewBuffer(Relation index);
extern void signValue(BloomState *state, SignType *sign, Datum value, int attno);
extern BloomTuple* BloomFormTuple(BloomState *state, ItemPointer iptr, Datum *values, bool *isnull);
bool BloomPageAddItem(BloomState *state, Page p, BloomTuple *t);
#endif
