package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/olekukonko/tablewriter"
)

func printJobsTable(data any) {
	jobs, ok := data.([]cluster.JobInfo)
	if !ok || len(jobs) == 0 {
		fmt.Println("No jobs found")
		return
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Submitted.Before(jobs[j].Submitted)
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Log URI", "Status", "Submitted", "Started", "Completed", "Cancelled"})
	for _, job := range jobs {
		table.Append([]string{
			job.ID,
			job.Spec.LogURI,
			string(job.Status),
			job.Submitted.Format("2006-01-02 15:04:05"),
			valOrDash(job.Started),
			valOrDash(job.Completed),
			valOrDash(job.Cancelled),
		})
	}
	table.Render()
}

func printJobStatusTable(data any) {
	var job cluster.JobInfo
	switch jt := data.(type) {
	case cluster.JobInfo:
		job = jt
	case *cluster.JobInfo:
		job = *jt
	default:
		fmt.Println("No job info")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Field", "Value"})
	table.Append([]string{"ID", job.ID})
	table.Append([]string{"Status", string(job.Status)})
	table.Append([]string{"Log URI", job.Spec.LogURI})
	table.Append([]string{"Submitted", job.Submitted.Format("2006-01-02 15:04:05")})
	table.Append([]string{"Started", valOrDash(job.Started)})
	table.Append([]string{"Completed", valOrDash(job.Completed)})
	table.Append([]string{"Cancelled", valOrDash(job.Cancelled)})
	table.Append([]string{"Note", job.Spec.Note})
	table.Render()
}

func printWorkersTable(data any) {
	workers, ok := data.([]api.WorkerStatus)
	if !ok || len(workers) == 0 {
		fmt.Println("No workers found")
		return
	}

	sort.Slice(workers, func(i, j int) bool {
		return workers[i].ID < workers[j].ID
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"ID", "Host", "Last Seen", "Shards Processed", "Shards Failed", "Processing Time (s)", "Last Updated",
	})
	for _, w := range workers {
		procTimeSec := float64(w.ProcessingTimeNs) / 1e9
		table.Append([]string{
			w.ID,
			w.Host,
			w.LastSeen.Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%d", w.ShardsProcessed),
			fmt.Sprintf("%d", w.ShardsFailed),
			fmt.Sprintf("%.2f", procTimeSec),
			w.LastUpdated.Format("2006-01-02 15:04:05"),
		})
	}
	table.Render()
}

func printWorkerMetricsTable(data any) {
	m, ok := data.(*cluster.WorkerMetricsView)
	if !ok || m == nil {
		fmt.Println("No worker metrics")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Worker ID", "Shards Processed", "Shards Failed", "Processing Time (s)", "Last Updated"})
	table.Append([]string{
		m.WorkerID,
		fmt.Sprintf("%d", m.ShardsProcessed),
		fmt.Sprintf("%d", m.ShardsFailed),
		fmt.Sprintf("%.2f", float64(m.ProcessingTimeNs)/1e9),
		m.LastUpdated.Format("2006-01-02 15:04:05"),
	})
	table.Render()
}

func printShardsTable(data any) {
	shards, ok := data.(map[int]cluster.ShardAssignmentStatus)
	if !ok || len(shards) == 0 {
		fmt.Println("No shards found")
		return
	}
	// Sort shard IDs
	var ids []int
	for id := range shards {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"Shard ID", "Worker ID", "Assigned", "Done", "Failed", "Lease Expiry", "Retries", "Backoff", "Idx From", "Idx To",
	})
	for _, id := range ids {
		s := shards[id]
		table.Append([]string{
			fmt.Sprintf("%d", id),
			s.WorkerID,
			fmt.Sprintf("%v", s.Assigned),
			fmt.Sprintf("%v", s.Done),
			fmt.Sprintf("%v", s.Failed),
			valOrDash(s.LeaseExpiry),
			fmt.Sprintf("%d", s.Retries),
			valOrDash(s.BackoffUntil),
			fmt.Sprintf("%d", s.IndexFrom),
			fmt.Sprintf("%d", s.IndexTo),
		})
	}
	table.Render()
}

func printShardStatusTable(data any) {
	status, ok := data.(cluster.ShardStatus)
	if !ok {
		fmt.Println("No shard status")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Field", "Value"})
	table.Append([]string{"Worker ID", status.WorkerID})
	table.Append([]string{"Assigned", fmt.Sprintf("%v", status.Assigned)})
	table.Append([]string{"Done", fmt.Sprintf("%v", status.Done)})
	table.Append([]string{"Failed", fmt.Sprintf("%v", status.Failed)})
	table.Append([]string{"Lease Expiry", valOrDash(status.LeaseExpiry)})
	table.Append([]string{"Retries", fmt.Sprintf("%d", status.Retries)})
	table.Append([]string{"Backoff", valOrDash(status.BackoffUntil)})
	table.Append([]string{"Index From", fmt.Sprintf("%d", status.IndexFrom)})
	table.Append([]string{"Index To", fmt.Sprintf("%d", status.IndexTo)})
	table.Render()
}

func printSecretsTable(data any) {
	keys, ok := data.([]string)
	if !ok || len(keys) == 0 {
		fmt.Println("No secrets found")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Secret Keys"})
	for _, key := range keys {
		table.Append([]string{key})
	}
	table.Render()
}

func printClusterStatusTable(data any) {
	status, ok := data.(*cluster.ClusterStatus)
	if !ok || status == nil {
		fmt.Println("No cluster status")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Field", "Value"})
	table.Append([]string{"Num Jobs", fmt.Sprintf("%d", len(status.Jobs))})
	table.Append([]string{"Num Workers", fmt.Sprintf("%d", len(status.Workers))})
	table.Render()

	// Show sub-tables for jobs and workers
	if len(status.Jobs) > 0 {
		fmt.Println("\nJobs:")
		for _, jobStatus := range status.Jobs {
			printJobStatusTable(jobStatus.Job)
		}
	}
	if len(status.Workers) > 0 {
		fmt.Println("\nWorkers:")
		printWorkersTable(status.Workers)
	}
}

func printPendingNodesTable(data any) {
	nodes, ok := data.([]secrets.PendingRegistration)
	if !ok || len(nodes) == 0 {
		fmt.Println("No pending nodes")
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Node ID", "Public Key (b64)"})
	for _, n := range nodes {
		table.Append([]string{n.NodeID, n.PubKeyB64})
	}
	table.Render()
}
