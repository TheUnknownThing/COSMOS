CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -pedantic
LDFLAGS ?=

.PHONY: all static clean

all: semantic_kernel

semantic_kernel: semantic_kernel.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

semantic_kernel_static: semantic_kernel.c
	$(CC) $(CFLAGS) -static -o $@ $< $(LDFLAGS)

static: semantic_kernel_static

clean:
	rm -f semantic_kernel semantic_kernel_static
