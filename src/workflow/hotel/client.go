package hotelworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func K6ClosedClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	userCount := common.MustInt(c.RunConfig, "hotel_user_count")
	hotelCount := common.MustInt(c.RunConfig, "hotel_hotel_count")
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runHotelK6(hotelK6RunConfig{
		duration:   duration,
		targetURL:  k6TargetURL,
		deadline:   k6CommandDeadline,
		sender:     c.Name,
		scriptPath: "workflow/hotel/k6_closed_client.js",
		extraEnv: []string{
			"HOTEL_VUS=" + strconv.Itoa(k6VUs),
			"HOTEL_USER_COUNT=" + strconv.Itoa(userCount),
			"HOTEL_HOTEL_COUNT=" + strconv.Itoa(hotelCount),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("hotel k6 closed client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("hotel k6 closed client failed: %v", err)
	}
}

type hotelK6RunConfig struct {
	duration   string
	targetURL  string
	deadline   time.Duration
	sender     string
	scriptPath string
	extraEnv   []string
}

func runHotelK6(config hotelK6RunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "TARGET_URL=" + config.targetURL,
		"-e", "SENDER=" + config.sender,
		"-e", "DURATION=" + config.duration,
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	cmd := exec.CommandContext(ctx, "k6", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		return fmt.Errorf("run k6: %w", err)
	}
	return nil
}
