package mediaworkflow

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

func K6ClosedReviewComposeClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	userCount := common.MustInt(c.RunConfig, "media_user_count")
	movieCount := common.MustInt(c.RunConfig, "media_movie_count")
	textLength := common.IntOrDefault(c.RunConfig, "media_review_text_length", 256)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runMediaK6(mediaK6RunConfig{
		duration:   duration,
		targetURL:  k6TargetURL,
		deadline:   k6CommandDeadline,
		sender:     c.Name,
		scriptPath: "workflow/media/k6_closed_review_compose.js",
		extraEnv: []string{
			"MEDIA_VUS=" + strconv.Itoa(k6VUs),
			"MEDIA_USER_COUNT=" + strconv.Itoa(userCount),
			"MEDIA_MOVIE_COUNT=" + strconv.Itoa(movieCount),
			"MEDIA_REVIEW_TEXT_LENGTH=" + strconv.Itoa(textLength),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("media k6 closed review compose client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("media k6 closed review compose client failed: %v", err)
	}
}

type mediaK6RunConfig struct {
	duration   string
	targetURL  string
	deadline   time.Duration
	sender     string
	scriptPath string
	extraEnv   []string
}

func runMediaK6(config mediaK6RunConfig) error {
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
