import argparse
import json
import logging
import os
import shlex
import subprocess
import time
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_experiment_topology(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    services = data.get("services", {})
    nodes = data.get("nodes", {})
    if not services or not nodes:
        raise ValueError("config must include non-empty 'services' and 'nodes'")

    node_names = sorted(nodes.keys())
    client_names = []
    for node_name, node_cfg in nodes.items():
        service_name = node_cfg.get("service")
        if not service_name:
            raise ValueError(f"node {node_name} is missing 'service'")

        service_cfg = services.get(service_name)
        if not service_cfg:
            raise ValueError(f"node {node_name} references unknown service '{service_name}'")

        if service_cfg.get("type") == "client":
            client_names.append(node_name)

    return node_names, sorted(client_names)


def collect_logs(node_names, client_names, results_dir="experiment/results"):
    logger.info("Collecting logs (%d nodes, %d clients)", len(node_names), len(client_names))
    def _scp(node_name, remote_path, local_path):
        return subprocess.run(["scp", f"{node_name}:{remote_path}", local_path])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(results_dir, timestamp)

    logs_dir = os.path.join(run_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    for name in node_names:
        local_path = os.path.join(logs_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)

    traces_dir = os.path.join(run_dir, "traces")
    os.makedirs(traces_dir, exist_ok=True)
    for name in client_names:
        local_path = os.path.join(traces_dir, f"{name}.jsonl")
        _scp(name, "/tmp/client_result.jsonl", local_path)
    logger.info("Log collection complete: %s", run_dir)

def launch_nodes(node_names, config_path):
    logger.info("Launching %d nodes", len(node_names))
    remote_config_path = f"../{config_path}"
    quoted_config_path = shlex.quote(remote_config_path)
    for name in node_names:
        subprocess.run(["ssh", name, "sh", "-lc",
                        "'cd /app/src && "
                        f"nohup go run . --name {name} --host 0.0.0.0 --port 8000 --config {quoted_config_path} "
                        "> /tmp/node.log 2>&1 &'"])

def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes", len(node_names))
    for name in node_names:
        subprocess.run(["ssh", name, "pkill -9 -f 'go'"])


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment")
    parser.add_argument("config_path", help="Path to architecture config JSON")
    args = parser.parse_args()

    node_names, client_names = load_experiment_topology(args.config_path)
    logger.info("Experiment starting")
    stop_docker_nodes(node_names)

    launch_nodes(node_names, args.config_path)
    logger.info("Waiting for nodes to run")
    time.sleep(90.0)

    stop_docker_nodes(node_names)
    collect_logs(node_names, client_names)
    logger.info("Experiment complete")


if __name__ == "__main__":
    main()
