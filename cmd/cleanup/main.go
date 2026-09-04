package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/truenas/truenas-csi/pkg/client"
)

type bulkResult struct {
	JobID  *int   `json:"job_id"`
	Error  string `json:"error"`
	Result any    `json:"result"`
}

type jobStatus struct {
	ID       int          `json:"id"`
	State    string       `json:"state"`
	Error    string       `json:"error"`
	Result   []bulkResult `json:"result"`
	Progress struct {
		Percent     int    `json:"percent"`
		Description string `json:"description"`
	} `json:"progress"`
}

func pollJob(ctx context.Context, c *client.Client, jobID int) (*jobStatus, error) {
	for {
		var jobs []jobStatus
		if err := c.Call(ctx, "core.get_jobs", []any{[]any{[]any{"id", "=", jobID}}}, &jobs); err != nil {
			return nil, fmt.Errorf("core.get_jobs failed: %w", err)
		}
		if len(jobs) == 0 {
			return nil, fmt.Errorf("job %d not found", jobID)
		}
		job := &jobs[0]
		switch job.State {
		case "SUCCESS":
			return job, nil
		case "FAILED":
			return job, fmt.Errorf("job failed: %s", job.Error)
		default:
			fmt.Printf("  Job %d: %s (%d%%)\n", jobID, job.State, job.Progress.Percent)
			time.Sleep(3 * time.Second)
		}
	}
}

func bulkDelete(ctx context.Context, c *client.Client, method string, paramSets [][]any) (int, int) {
	if len(paramSets) == 0 {
		fmt.Println("  Nothing to delete")
		return 0, 0
	}

	fmt.Printf("  Sending core.bulk %s with %d items...\n", method, len(paramSets))

	// core.bulk returns a job ID
	var jobID int
	err := c.Call(ctx, "core.bulk", []any{method, paramSets}, &jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  core.bulk call failed: %v\n", err)
		return 0, len(paramSets)
	}
	fmt.Printf("  Job ID: %d, waiting for completion...\n", jobID)

	// Poll until job completes
	job, err := pollJob(ctx, c, jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %v\n", err)
		return 0, len(paramSets)
	}

	ok, fail := 0, 0
	for i, r := range job.Result {
		if r.Error != "" {
			fmt.Fprintf(os.Stderr, "  [%d] Error: %s\n", i, r.Error)
			fail++
		} else {
			ok++
		}
	}
	fmt.Printf("  Done: %d succeeded, %d failed\n", ok, fail)
	return ok, fail
}

func main() {
	url := os.Getenv("TRUENAS_URL")
	if url == "" {
		url = fmt.Sprintf("wss://%s/api/current", os.Getenv("TRUENAS_IP"))
	}
	apiKey := os.Getenv("TRUENAS_API_KEY")
	pool := os.Getenv("TRUENAS_POOL")
	if pool == "" {
		pool = "tank"
	}

	c := client.New(client.Config{
		URL:                url,
		APIKey:             apiKey,
		InsecureSkipVerify: true,
	})
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected to TrueNAS")

	// 1. Delete all iSCSI target-extent associations
	fmt.Println("\n=== Deleting iSCSI target-extent associations ===")
	var targetExtents []struct {
		ID int `json:"id"`
	}
	if err := c.Call(ctx, "iscsi.targetextent.query", []any{}, &targetExtents); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to query target-extents: %v\n", err)
	} else {
		params := make([][]any, 0, len(targetExtents))
		for _, te := range targetExtents {
			params = append(params, []any{te.ID, true})
		}
		bulkDelete(ctx, c, "iscsi.targetextent.delete", params)
	}

	// 2. Delete all iSCSI extents
	fmt.Println("\n=== Deleting iSCSI extents ===")
	var extents []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err := c.Call(ctx, "iscsi.extent.query", []any{}, &extents); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to query extents: %v\n", err)
	} else {
		params := make([][]any, 0, len(extents))
		for _, e := range extents {
			params = append(params, []any{e.ID, true, true})
		}
		fmt.Printf("  Found %d extents\n", len(params))
		bulkDelete(ctx, c, "iscsi.extent.delete", params)
	}

	// 3. Delete all iSCSI targets with "csi" prefix
	fmt.Println("\n=== Deleting iSCSI targets ===")
	var targets []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err := c.Call(ctx, "iscsi.target.query", []any{}, &targets); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to query targets: %v\n", err)
	} else {
		params := make([][]any, 0, len(targets))
		for _, t := range targets {
			if strings.HasPrefix(t.Name, "csi-") {
				params = append(params, []any{t.ID, true})
			}
		}
		fmt.Printf("  Found %d CSI targets (of %d total)\n", len(params), len(targets))
		bulkDelete(ctx, c, "iscsi.target.delete", params)
	}

	// 4. Delete all pvc- datasets/ZVOLs under the pool
	fmt.Println("\n=== Deleting orphaned datasets ===")
	var datasets []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := c.Call(ctx, "pool.dataset.query", []any{[]any{[]string{"pool", "=", pool}}}, &datasets); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to query datasets: %v\n", err)
	} else {
		params := make([][]any, 0)
		for _, d := range datasets {
			if strings.Contains(d.ID, "/pvc-") || strings.Contains(d.ID, "/snapshot-") {
				params = append(params, []any{d.ID, map[string]any{"recursive": true, "force": true}})
			}
		}
		fmt.Printf("  Found %d orphaned datasets (of %d total)\n", len(params), len(datasets))
		bulkDelete(ctx, c, "pool.dataset.delete", params)
	}

	fmt.Println("\n=== Cleanup complete ===")
}
