# contrib/planner/Makefile

MODULE_big = planner
OBJS = \
	$(WIN32RES) \
	planner.o \
	utils.o
PGFILEDESC = "planner"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/planner
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
