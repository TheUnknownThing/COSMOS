// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <bpf/bpf_helpers.h>

struct flow_policy {
	__u32 priority;
	__u32 _pad;
	__u64 rate_bps;
};

struct token_bucket {
	__u64 tokens;
	__u64 last_ns;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 4096);
	__type(key, __u64);
	__type(value, struct flow_policy);
} flow_policies SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 4096);
	__type(key, __u64);
	__type(value, struct token_bucket);
} token_buckets SEC(".maps");

SEC("classifier")
int cosmos_net_tc(struct __sk_buff *skb)
{
	__u64 cgroup_id = bpf_skb_cgroup_id(skb);
	struct flow_policy *policy;
	struct token_bucket *bucket;
	struct token_bucket initial;
	__u64 now;
	__u64 pkt_bits;
	__u64 cap;
	__u64 refill;
	__u64 elapsed;

	if (cgroup_id == 0)
		return TC_ACT_OK;

	policy = bpf_map_lookup_elem(&flow_policies, &cgroup_id);
	if (!policy)
		return TC_ACT_OK;

	if (policy->priority)
		skb->priority = policy->priority;

	if (policy->rate_bps == 0)
		return TC_ACT_OK;

	now = bpf_ktime_get_ns();
	pkt_bits = ((__u64)skb->len) * 8;
	cap = policy->rate_bps / 10;
	if (cap < pkt_bits)
		cap = pkt_bits;

	bucket = bpf_map_lookup_elem(&token_buckets, &cgroup_id);
	if (!bucket) {
		initial.tokens = cap > pkt_bits ? cap - pkt_bits : 0;
		initial.last_ns = now;
		bpf_map_update_elem(&token_buckets, &cgroup_id, &initial, BPF_ANY);
		return TC_ACT_OK;
	}

	elapsed = now - bucket->last_ns;
	refill = elapsed * policy->rate_bps / 1000000000ULL;
	if (refill > 0) {
		bucket->tokens += refill;
		if (bucket->tokens > cap)
			bucket->tokens = cap;
		bucket->last_ns = now;
	}

	if (bucket->tokens < pkt_bits)
		return TC_ACT_SHOT;

	bucket->tokens -= pkt_bits;
	return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";
