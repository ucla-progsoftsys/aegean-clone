package nodes

import "testing"

func TestClientStatusHandlerReflectsRequestLogicState(t *testing.T) {
	client := NewClient(
		"client",
		"127.0.0.1",
		8000,
		nil,
		nil,
		map[string]any{},
		"test_client",
		func(c *Client) {},
	)

	if client.Node.HandleStatus == nil {
		t.Fatal("expected /status handler to be registered")
	}

	status := client.HandleStatus(map[string]any{})
	if started, _ := status["request_logic_started"].(bool); started {
		t.Fatalf("expected request_logic_started=false, got %v", status)
	}
	if completed, _ := status["request_logic_completed"].(bool); completed {
		t.Fatalf("expected request_logic_completed=false, got %v", status)
	}

	client.requestLogicStarted.Store(true)
	client.requestLogicCompleted.Store(true)

	status = client.HandleStatus(map[string]any{})
	if started, _ := status["request_logic_started"].(bool); !started {
		t.Fatalf("expected request_logic_started=true, got %v", status)
	}
	if completed, _ := status["request_logic_completed"].(bool); !completed {
		t.Fatalf("expected request_logic_completed=true, got %v", status)
	}
}
