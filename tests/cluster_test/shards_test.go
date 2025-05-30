package cluster_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestBulkCreateShards_ExactShardCountAndIDs(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "manyshards"
	numShards := 1000
	var shards []cluster.ShardRange
	for i := 0; i < numShards; i++ {
		shards = append(shards, cluster.ShardRange{ShardID: i, IndexFrom: int64(i * 100), IndexTo: int64((i + 1) * 100)})
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// Test GetShardAssignments returns all shard IDs
	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, statusMap, numShards)
	for i := 0; i < numShards; i++ {
		stat, ok := statusMap[i]
		require.True(t, ok, "missing shard %d", i)
		require.Equal(t, int64(i*100), stat.IndexFrom)
		require.Equal(t, int64((i+1)*100), stat.IndexTo)
		require.False(t, stat.Assigned)
		require.False(t, stat.Done)
	}

	// Test GetShardAssignmentsWindow on full range and last/first windows
	win, err := cl.GetShardAssignmentsWindow(ctx, jobID, 0, numShards)
	require.NoError(t, err)
	require.Len(t, win, numShards)
	// Spot check boundary windows
	winFirst, err := cl.GetShardAssignmentsWindow(ctx, jobID, 0, 10)
	require.NoError(t, err)
	require.Len(t, winFirst, 10)
	winLast, err := cl.GetShardAssignmentsWindow(ctx, jobID, numShards-10, numShards)
	require.NoError(t, err)
	require.Len(t, winLast, 10)
	for i := numShards - 10; i < numShards; i++ {
		_, ok := winLast[i]
		require.True(t, ok, "missing shard %d in last window", i)
	}
}

func TestBulkCreateShards_LateCreateOfHighestShard(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "partialshards"
	var shards []cluster.ShardRange
	for i := 0; i < 999; i++ {
		shards = append(shards, cluster.ShardRange{ShardID: i, IndexFrom: int64(i * 100), IndexTo: int64((i + 1) * 100)})
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))
	// Now add just 999
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{
		{ShardID: 999, IndexFrom: 99900, IndexTo: 100000},
	}))

	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, statusMap, 1000)
	// All present
	for i := 0; i < 1000; i++ {
		_, ok := statusMap[i]
		require.True(t, ok, "missing shard %d", i)
	}
}

func TestBulkCreateAndShardAssignmentLifecycle(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	jobID := "testjob"
	shards := []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 1000},
		{ShardID: 1, IndexFrom: 1000, IndexTo: 2000},
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// Idempotency
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// No assignment/done yet
	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, statusMap, 2)
	for _, s := range shards {
		stat := statusMap[s.ShardID]
		require.Equal(t, s.IndexFrom, stat.IndexFrom)
		require.Equal(t, s.IndexTo, stat.IndexTo)
		require.False(t, stat.Assigned)
		require.False(t, stat.Done)
		require.False(t, stat.Failed)
	}

	// Assign a shard
	workerID := "worker1"
	require.NoError(t, cl.AssignShard(ctx, jobID, 0, workerID))

	// Assignment status updates
	stat, err := cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat.Assigned)
	require.Equal(t, workerID, stat.WorkerID)
	require.False(t, stat.Done)
	require.False(t, stat.Failed)

	// Mark done
	manifest := cluster.ShardManifest{OutputPath: "/tmp/shard0.jsonl"}
	require.NoError(t, cl.ReportShardDone(ctx, jobID, 0, manifest))
	stat, err = cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat.Done)
	require.Equal(t, "/tmp/shard0.jsonl", stat.OutputPath)

	// Assign and fail the other shard (with retries/backoff)
	require.NoError(t, cl.AssignShard(ctx, jobID, 1, "worker2"))
	for i := 0; i < 4; i++ {
		err := cl.ReportShardFailed(ctx, jobID, 1)
		if i < 3 {
			require.NoError(t, err)
			s, _ := cl.GetShardStatus(ctx, jobID, 1)
			require.Equal(t, i+1, s.Retries)
			require.False(t, s.Failed)
			require.False(t, s.Done)
		} else {
			require.NoError(t, err)
			s, _ := cl.GetShardStatus(ctx, jobID, 1)
			require.True(t, s.Failed, "should be permanently failed after max retries")
			require.True(t, s.Done)
		}
	}
}

func TestRequestShardSplit(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "splitjob"
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{
		{ShardID: 10, IndexFrom: 0, IndexTo: 10000},
	}))
	err := cl.RequestShardSplit(ctx, jobID, 10, []cluster.ShardRange{
		{ShardID: 11, IndexFrom: 0, IndexTo: 5000},
		{ShardID: 12, IndexFrom: 5000, IndexTo: 10000},
	})
	require.NoError(t, err)

	// Check the split flag
	prefix := cl.ShardKey("splitjob", 10) + "/split"
	resp, err := cl.Client().Get(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)

	// Check new shards: verify statusMap includes their /range records
	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	_, ok11 := statusMap[11]
	_, ok12 := statusMap[12]
	require.True(t, ok11, "Shard 11 missing from assignments")
	require.True(t, ok12, "Shard 12 missing from assignments")
	// Check the range info is as expected
	require.Equal(t, int64(0), statusMap[11].IndexFrom)
	require.Equal(t, int64(5000), statusMap[11].IndexTo)
	require.Equal(t, int64(5000), statusMap[12].IndexFrom)
	require.Equal(t, int64(10000), statusMap[12].IndexTo)
}

func TestReassignOrphanedShards(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "orphanjob"
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{{ShardID: 0, IndexFrom: 0, IndexTo: 100}}))

	// Simulate orphan by assigning and then "expiring" lease
	require.NoError(t, cl.AssignShard(ctx, jobID, 0, "deadworker"))
	// Manually set lease expiry in the past
	prefix := cl.ShardKey("orphanjob", 0) + "/assignment"
	resp, err := cl.Client().Get(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)
	var a cluster.ShardAssignment
	require.NoError(t, json.Unmarshal(resp.Kvs[0].Value, &a))
	a.LeaseExpiry = time.Now().Add(-10 * time.Minute)
	b, _ := json.Marshal(a)
	_, err = cl.Client().Put(ctx, prefix, string(b))
	require.NoError(t, err)

	// Should now be orphaned
	orphans, err := cl.FindOrphanedShards(ctx, jobID)
	require.NoError(t, err)
	require.Contains(t, orphans, 0)

	reassigned, err := cl.ReassignOrphanedShards(ctx, jobID, "newworker")
	require.NoError(t, err)
	require.Contains(t, reassigned, 0)

	// Should no longer be orphaned
	orphansAfter, err := cl.FindOrphanedShards(ctx, jobID)
	require.NoError(t, err)
	require.NotContains(t, orphansAfter, 0)
}

func TestCluster_OrphanedShardRecovery(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 1)
	shardID := 0

	// Assign the shard, then expire its lease
	require.NoError(t, cl.AssignShard(ctx, jobID, shardID, "oldworker"))
	testcluster.ExpireShardLease(t, cl, jobID, shardID)

	// Should now be orphaned
	orphans, err := cl.FindOrphanedShards(ctx, jobID)
	require.NoError(t, err)
	require.Contains(t, orphans, shardID)

	// Now attempt to reassign via ReassignOrphanedShards
	reassigned, err := cl.ReassignOrphanedShards(ctx, jobID, "newworker")
	require.NoError(t, err)
	require.Contains(t, reassigned, shardID)

	// Should not be orphaned anymore
	orphansAfter, err := cl.FindOrphanedShards(ctx, jobID)
	require.NoError(t, err)
	require.NotContains(t, orphansAfter, shardID)
}

func TestGetShardAssignmentsWindow(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "windowjob"
	var shards []cluster.ShardRange
	for i := 0; i < 10; i++ {
		shards = append(shards, cluster.ShardRange{ShardID: i, IndexFrom: int64(i * 1000), IndexTo: int64((i + 1) * 1000)})
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// Only get window 3-6
	window, err := cl.GetShardAssignmentsWindow(ctx, jobID, 3, 7)
	require.NoError(t, err)
	require.Len(t, window, 4)
	for k := 3; k <= 6; k++ {
		stat, ok := window[k]
		require.True(t, ok, "missing shard %d", k)
		require.False(t, stat.Assigned)
		require.False(t, stat.Done)
	}
}

func TestGetShardCount_ZeroIfNotSet(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	count, err := cl.GetShardCount(ctx, "doesnotexist")
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestGetShardAssignmentsWindow_EmptyOnOutOfBounds(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "outofboundsjob"
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 100},
	}))
	window, err := cl.GetShardAssignmentsWindow(ctx, jobID, 10, 20)
	require.NoError(t, err)
	require.Len(t, window, 0)
}

func TestRenewShardLease(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "renewleasejob"

	// Create a shard and assign it to a worker
	shards := []cluster.ShardRange{{ShardID: 0, IndexFrom: 0, IndexTo: 100}}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))
	workerID := "worker123"
	require.NoError(t, cl.AssignShard(ctx, jobID, 0, workerID))

	// Fetch initial assignment & lease expiry
	stat, err := cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat.Assigned)
	oldExpiry := stat.LeaseExpiry

	// Wait a small amount of time to guarantee lease changes
	time.Sleep(10 * time.Millisecond)

	// Renew lease as correct worker
	require.NoError(t, cl.RenewShardLease(ctx, jobID, 0, workerID))
	stat2, err := cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat2.Assigned)
	require.Equal(t, workerID, stat2.WorkerID)
	require.True(t, stat2.LeaseExpiry.After(oldExpiry), "Lease expiry should increase after renewal")

	// Try to renew lease as wrong worker
	err = cl.RenewShardLease(ctx, jobID, 0, "badworker")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not own shard", "should not allow non-owner to renew")

	// Unassign the shard manually, then try to renew (should fail)
	// Simulate unassigned by deleting assignment
	assignKey := cl.ShardKey(jobID, 0) + "/assignment"
	_, err = cl.Client().Delete(ctx, assignKey)
	require.NoError(t, err)
	err = cl.RenewShardLease(ctx, jobID, 0, workerID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "assignment not found", "should fail to renew if not assigned")
}
