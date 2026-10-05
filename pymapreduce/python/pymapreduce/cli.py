import argparse
import asyncio
import pymapreduce
import sys
import os
import signal

async def _wait_for_exit():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    
    def _signal_handler():
        stop_event.set()
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass # Windows doesn't support add_signal_handler fully, but we are on Linux

    await stop_event.wait()

def start_head(port, scheduler_name):
    async def _run_head():
        print(f"[MapReduce] Starting Head Node at 0.0.0.0:{port} with Scheduler: {scheduler_name}...")
        address = f"0.0.0.0:{port}"
        scheduler = getattr(pymapreduce.SchedulerStrategy, scheduler_name, pymapreduce.SchedulerStrategy.RoundRobin)
        await pymapreduce.DriverWrapper.init(address=address, scheduler=scheduler)
        print("[MapReduce] Head Node is running successfully. Press Ctrl+C to exit.")
        
        await _wait_for_exit()
            
    try:
        asyncio.run(_run_head())
    except KeyboardInterrupt:
        pass
    print("\n[MapReduce] Head Node shut down.")

def start_worker(head_addr, cpus, idle_timeout):
    async def _run_worker():
        if cpus is None:
            try:
                num_threads = os.cpu_count() or 1
            except:
                num_threads = 1
            print(f"[MapReduce] Starting Worker Node connecting to {head_addr} (Auto-detected: {num_threads} cores)...")
        else:
            print(f"[MapReduce] Starting Worker Node connecting to {head_addr} with {cpus} cores...")
            
        if idle_timeout > 0:
            print(f"[MapReduce] Idle timeout set to {idle_timeout} seconds")
        else:
            print(f"[MapReduce] Idle timeout disabled (processes live forever)")
            
        await pymapreduce.start_worker(head_addr, cpus, idle_timeout)
        
        print("[MapReduce] Worker Node is running successfully. Press Ctrl+C to exit.")
        
        await _wait_for_exit()
            
    try:
        asyncio.run(_run_worker())
    except KeyboardInterrupt:
        pass
    print("\n[MapReduce] Worker Node shut down.")

def main():
    parser = argparse.ArgumentParser(description="MapReduce AutoML CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    start_parser = subparsers.add_parser("start", help="Start the MapReduce cluster")

    start_parser.add_argument("--head", action="store_true", help="Start as Head Node")
    start_parser.add_argument("--worker", action="store_true", help="Start as Worker Node")
    start_parser.add_argument("--head-addr", type=str, default="127.0.0.1:7777", help="IP address of the Head Node (e.g., 192.168.1.100:7777)")
    start_parser.add_argument("--port", type=int, default=7777, help="Port for the Head Node to listen on (default: 7777)")
    start_parser.add_argument("--cpus", type=int, default=None, help="Number of CPU cores the worker should use. Defaults to all available cores.")
    start_parser.add_argument("--idle-timeout", type=int, default=0, help="Kill worker processes if idle for this many seconds (0 = never timeout).")
    start_parser.add_argument("--scheduler", type=str, default="RoundRobin", choices=["RoundRobin", "WeightedCapacity", "LeastLoad", "LocalityFirst", "Adaptive"], help="Scheduling strategy for Head Node")

    submit_parser = subparsers.add_parser("submit", help="Submit a Python script to the cluster")
    submit_parser.add_argument("script", type=str, help="Python script to execute")
    submit_parser.add_argument("--head-addr", type=str, default="127.0.0.1:7777", help="IP address of the Head Node")
    submit_parser.add_argument("--max-time", type=int, default=None, help="Maximum execution time limit in seconds")

    args = parser.parse_args()

    if args.command == "start":
        if args.head:
            start_head(args.port, args.scheduler)
        elif args.worker:
            start_worker(args.head_addr, args.cpus, args.idle_timeout)
        else:
            print("Please specify --head or --worker. Example: mapreduce start --head")
            sys.exit(1)
    elif args.command == "submit":
        if args.max_time:
            os.environ["PYMAPREDUCE_MAX_TIME"] = str(args.max_time)
        import runpy
        sys.argv = [args.script]
        runpy.run_path(args.script, run_name="__main__")
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
