#!/usr/bin/env python3
"""Local four-replica batching/load regression. Requires built binaries and localhost ports."""

import argparse
import json
from pathlib import Path
import platform
import re
import signal
import subprocess
import tempfile
import time


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=root / "build")
    parser.add_argument("--protocol", choices=["pbft", "sbft", "both"], default="both")
    parser.add_argument("--rate", type=int, default=40000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--base-port", type=int, default=19440)
    parser.add_argument("--connections", type=int, default=4)
    parser.add_argument("--target-mode", choices=["leader", "broadcast"], default="broadcast",
                        help="client request routing (default: broadcast, matching CloudLab)")
    parser.add_argument("--proposal-interval", type=int, default=100)
    parser.add_argument("--batch-max-requests", type=int, default=8192)
    parser.add_argument("--max-pending-requests", type=int, default=32768)
    parser.add_argument("--expect-rejections", action="store_true")
    parser.add_argument("--allow-rejections", action="store_true")
    parser.add_argument("--initial-timeout", type=int, help="replica election watchdog in milliseconds")
    parser.add_argument("--pause-replica", type=int, choices=range(4))
    args = parser.parse_args()
    if min(args.rate, args.duration, args.connections, args.proposal_interval,
           args.batch_max_requests, args.max_pending_requests) <= 0:
        parser.error("rates, durations, and limits must be positive")
    if args.initial_timeout is not None and args.initial_timeout <= 0:
        parser.error("initial timeout must be positive")
    if args.pause_replica is not None and args.duration < 6:
        parser.error("pause/resume tests require --duration >= 6")
    build = args.build_dir.resolve()
    for binary in ["CppBedrock", "bedrock_client"]:
        if not (build / binary).is_file():
            parser.error(f"missing {build / binary}; build CppBedrock first")
    run = Path(tempfile.mkdtemp(prefix="bedrock-batching-"))
    print(f"Logs: {run}", flush=True)
    subprocess.run(["bash", str(root / "keys/gen_server_keys.sh"), "4", str(run / "keys")],
                   check=True, stdout=subprocess.DEVNULL)
    results = {"platform": platform.platform(), "options": vars(args).copy(), "cases": {}}
    results["options"]["build_dir"] = str(build)
    modes = ["pbft", "sbft"] if args.protocol == "both" else [args.protocol]
    for mode in modes:
        case = run / mode
        case.mkdir()
        committee = case / "committee.json"
        committee.write_text(json.dumps({"replicas": [
            {"id": i, "host": "127.0.0.1", "port": args.base_port + i} for i in range(4)]}))
        processes, logs = [], []
        client = None
        try:
            for i in range(4):
                out = (case / f"server_{i}.log").open("w")
                logs.append(out)
                processes.append(subprocess.Popen([
                    str(build / "CppBedrock"), "--node-id", str(i), "--committee", str(committee),
                    "--protocol-config", str(root / f"config/config.{mode}.yaml"),
                    "--keys-dir", str(run / "keys"), "--stats-interval", "500",
                    "--proposal-interval", str(args.proposal_interval),
                    "--batch-max-requests", str(args.batch_max_requests),
                    "--max-pending-requests", str(args.max_pending_requests),
                    "--initial-timeout", str(args.initial_timeout or (1200 if args.pause_replica is not None else 8000)),
                ], stdout=out, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + 10
            while not all("gRPC listening" in (case / f"server_{i}.log").read_text() for i in range(4)):
                if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
                    raise RuntimeError(f"replica startup failed; inspect {case}")
                time.sleep(0.05)
            out = (case / "client.log").open("w")
            logs.append(out)
            client = subprocess.Popen([
                str(build / "bedrock_client"), "--committee", str(committee),
                "--rate", str(args.rate), "--duration", str(args.duration),
                "--connections", str(args.connections), "--request-timeout", "10000",
                "--target-mode", args.target_mode,
            ], stdout=out, stderr=subprocess.STDOUT)
            if args.pause_replica is not None:
                time.sleep(1)
                processes[args.pause_replica].send_signal(signal.SIGSTOP)
                time.sleep(3)
                processes[args.pause_replica].send_signal(signal.SIGCONT)
            client.wait(timeout=args.duration + 25)
            text = (case / "client.log").read_text()
            match = re.search(r"Benchmark results:\s*([^\n]+)", text)
            if client.returncode or not match:
                raise RuntimeError(f"client failed; inspect {case}")
            metrics = dict(re.findall(r"(\w+)=([\d.]+)", match[1]))
            terminal = re.search(r"stream_failures=(\d+) rejected=(\d+) timed_out=(\d+)", text)
            if not terminal or int(terminal[1]) or int(terminal[3]):
                raise RuntimeError(f"stream failure or request timeout; inspect {case}")
            if args.expect_rejections:
                if not int(terminal[2]) or not int(metrics["succ"]):
                    raise RuntimeError("expected both overload rejections and successful progress")
            elif int(metrics["err"]) and not args.allow_rejections:
                raise RuntimeError(f"unexpected client errors; inspect {case}")
            if args.pause_replica is None and int(metrics["trxs"]) < args.rate * args.duration * 0.99:
                raise RuntimeError("client did not sustain the requested offered rate")
            target = int(metrics["succ"])
            deadline = time.monotonic() + 10
            while True:
                caught_up = []
                for i in range(4):
                    stats = re.findall(r"Stats .*committed_transactions=(\d+)",
                                       (case / f"server_{i}.log").read_text())
                    caught_up.append(bool(stats) and int(stats[-1]) == target)
                if all(caught_up):
                    break
                if time.monotonic() >= deadline or any(p.poll() is not None for p in processes):
                    raise RuntimeError(f"replicas did not converge to {target} transactions; inspect {case}")
                time.sleep(0.1)
            if args.pause_replica is None:
                for i in range(4):
                    if "view change to " in (case / f"server_{i}.log").read_text():
                        raise RuntimeError(f"unexpected view change under load; inspect {case}")
            results["cases"][mode] = metrics
            (run / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"{mode}: {match[1]}", flush=True)
        finally:
            if client and client.poll() is None:
                client.kill()
                client.wait()
            for process in processes:
                if process.poll() is None:
                    process.send_signal(signal.SIGCONT)
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
