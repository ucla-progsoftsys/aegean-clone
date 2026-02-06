package main

type NodeConfig struct {
	Type             string
	Next             []string
	Clients          []string
	Shim             string
	Verifiers        []string
	Peers            []string
	Execs            []string
	IsPrimaryBatcher bool
}

var config = map[string]NodeConfig{
	"node1": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node2": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node3": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node4": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node5", "node6"},
		IsPrimaryBatcher: true,
	},
	"node5": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node4", "node6"},
		IsPrimaryBatcher: false,
	},
	"node6": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node4", "node5"},
		IsPrimaryBatcher: false,
	},
}
