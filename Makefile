MODULE_big = bloom
OBJS = blutils.o blinsert.o blscan.o blvacuum.o blcost.o

DATA_built = bloom.sql
DATA = uninstall_bloom.sql
REGRESS = bloom

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bloom
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
