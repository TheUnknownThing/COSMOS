CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -pedantic
LDFLAGS ?=

.PHONY: all static clean

KERNELS := \
	semantic_control \
	semantic_passive_wait \
	semantic_network_wait \
	semantic_local_io \
	semantic_memory_touch \
	semantic_cpu_loop \
	semantic_mixed_pipeline \
	semantic_workflow_fanout \
	semantic_cpu_spin \
	semantic_memory_scan \
	semantic_storage_io \
	semantic_network_transfer \
	semantic_balanced_pipeline

STATIC_KERNELS := $(addsuffix _static,$(KERNELS))

all: $(KERNELS)

$(KERNELS): %: %.c semantic_kernel_common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(STATIC_KERNELS): %_static: %.c semantic_kernel_common.h
	$(CC) $(CFLAGS) -static -o $@ $< $(LDFLAGS)

static: $(STATIC_KERNELS)

clean:
	rm -f $(KERNELS) $(STATIC_KERNELS) semantic_kernel semantic_kernel_static
