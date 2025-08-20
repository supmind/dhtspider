import pytest
from dhtspider.kbucket import KBucket, RoutingTable, K

# Helper function to generate a dummy node ID
def make_node_id(i):
    return i.to_bytes(20, 'big')

def test_kbucket_add_node():
    bucket = KBucket(0, 2**160 - 1)
    node_id = make_node_id(1)
    bucket.add_node((node_id, "127.0.0.1", 6881))
    assert len(bucket) == 1
    assert node_id in bucket.nodes

def test_kbucket_add_existing_node():
    bucket = KBucket(0, 2**160 - 1)
    node_id = make_node_id(1)
    node = (node_id, "127.0.0.1", 6881)
    bucket.add_node(node)

    # Add another node to be the oldest
    bucket.add_node((make_node_id(2), "127.0.0.1", 6882))

    # Re-adding the first node should move it to the end (most recent)
    bucket.add_node(node)

    assert list(bucket.nodes.keys())[0] == make_node_id(2)
    assert list(bucket.nodes.keys())[1] == make_node_id(1)

def test_kbucket_full_eviction():
    bucket = KBucket(0, 2**160 - 1)

    # Fill the bucket to capacity (K)
    for i in range(K):
        bucket.add_node((make_node_id(i), "127.0.0.1", 6881 + i))

    assert len(bucket) == K

    # Add one more node, which should evict the oldest one (node 0)
    new_node_id = make_node_id(K)
    bucket.add_node((new_node_id, "127.0.0.1", 6881 + K))

    assert len(bucket) == K
    assert make_node_id(0) not in bucket.nodes
    assert new_node_id in bucket.nodes

def test_routing_table_initialization():
    our_id = make_node_id(100)
    table = RoutingTable(our_id)
    assert len(table.buckets) == 1
    assert table.buckets[0].min_id == 0
    assert table.buckets[0].max_id == 2**160 - 1

def test_routing_table_add_node():
    our_id = make_node_id(100)
    table = RoutingTable(our_id)
    node_id = make_node_id(1)
    table.add_node((node_id, "127.0.0.1", 6881))

    assert len(table.buckets[0]) == 1
    assert node_id in table.buckets[0].nodes

def test_routing_table_split_bucket():
    # Choose an ID that will be in the first bucket
    our_id = make_node_id(1)
    table = RoutingTable(our_id)

    # Add K nodes that fall into the same bucket as our_id to trigger a split
    # These nodes should have IDs close to our_id
    for i in range(2, K + 2):
        # All these IDs will fall in the initial bucket
        node_id = make_node_id(i)
        table.add_node((node_id, "127.0.0.1", 6881 + i))

    # The bucket should have split
    assert len(table.buckets) == 2

    # Check if nodes are correctly redistributed
    our_id_int = int.from_bytes(our_id, 'big')

    # Find which bucket our node is in now
    our_bucket_index = -1
    for i, bucket in enumerate(table.buckets):
        if bucket.min_id <= our_id_int < bucket.max_id:
            our_bucket_index = i
            break

    assert our_bucket_index != -1, "Our node should be in one of the buckets"

    # The buckets should be sorted by their min_id
    table.buckets.sort(key=lambda b: b.min_id)

    # Check if the split was correct
    assert table.buckets[0].max_id == table.buckets[1].min_id
    assert table.buckets[0].min_id == 0
    assert table.buckets[1].max_id == 2**160 -1

    # Check that all original nodes are still in the table
    total_nodes = sum(len(b) for b in table.buckets)
    assert total_nodes == K, f"Expected {K} nodes, but found {total_nodes}"


def test_routing_table_get_closest_nodes_simple():
    our_id = make_node_id(0)
    table = RoutingTable(our_id)

    # Add a few nodes
    table.add_node((make_node_id(1), "127.0.0.1", 6881))
    table.add_node((make_node_id(2), "127.0.0.1", 6882))
    table.add_node((make_node_id(3), "127.0.0.1", 6883))

    # Target ID is 2
    target_id = make_node_id(2)
    # Get 3 closest nodes
    closest_nodes = table.get_closest_nodes(target_id, count=3)

    assert len(closest_nodes) == 3

    closest_node_ids = [int.from_bytes(n[0], 'big') for n in closest_nodes]

    # Distances from 2:
    # 2^2 = 0
    # 2^3 = 1
    # 2^1 = 3
    # Expected order: 2, 3, 1
    assert closest_node_ids == [2, 3, 1]
