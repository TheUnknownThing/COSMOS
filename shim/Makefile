# COSMOS Invocation Metadata Shim Library
#
# Builds libcosmos_meta.so which can be:
#   1. Linked directly into a FaaS runtime
#   2. LD_PRELOAD'd into any process to inject metadata
#
# Usage:
#   make              # Build libcosmos_meta.so
#   make test_shim    # Build standalone test binary
#   make clean        # Remove build artifacts

CC      ?= gcc
CFLAGS  := -Wall -Wextra -O2 -fPIC
LDFLAGS := -shared

.PHONY: all clean

all: libcosmos_meta.so

libcosmos_meta.so: cosmos_meta.c cosmos_preload.c cosmos_meta.h
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ cosmos_meta.c cosmos_preload.c

test_shim: test_shim.c cosmos_meta.c cosmos_meta.h
	$(CC) $(CFLAGS) -o $@ test_shim.c cosmos_meta.c

clean:
	rm -f libcosmos_meta.so test_shim
