#!/usr/bin/env python3
"""
run.py
Convenience script to start all nodes and the controller.

Usage:
  python run.py                    # Start all nodes + controller with default test file
  python run.py --csv mytest.csv   # Start with custom test file
  python run.py --nodes-only       # Start nodes only (no controller)
  python run.py --controller-only  # Start controller only
  python run.py --set 5            # Run only set 5
  python run.py --set 1-5          # Run sets 1 through 5
  python run.py --skip 2,4         # Skip sets 2 and 4
  python run.py --quiet            # Reduce node logging (cleaner output)
"""
import subprocess
import sys
import time
import os
import argparse
import signal

def main():
    parser = argparse.ArgumentParser(description="Run distributed transaction system")
    parser.add_argument("--csv", default="testcases.csv", help="Test cases CSV file")
    parser.add_argument("--nodes-only", action="store_true", help="Start nodes only")
    parser.add_argument("--controller-only", action="store_true", help="Start controller only")
    parser.add_argument("--db-dir", default="db", help="Database directory")
    parser.add_argument("--set", "-s", dest="run_sets", default="",
                        help="Run only specific sets (e.g., '5', '1,3,5', '1-5', '1-3,7')")
    parser.add_argument("--skip", dest="skip_sets", default="",
                        help="Skip specific sets (e.g., '2,4', '1-3')")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Reduce node logging (only warnings/errors)")
    args = parser.parse_args()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    db_dir = os.path.join(script_dir, args.db_dir)
    if os.path.exists(db_dir):
        print(f"Cleaning up database directory: {db_dir}")
        import shutil
        shutil.rmtree(db_dir)
    
    processes = []
    
    def cleanup(signum=None, frame=None):
        print("\nShutting down...")
        for p in processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except:
                try:
                    p.kill()
                except:
                    pass
        sys.exit(0)
    
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    try:
        if not args.controller_only:
            node_ids = ["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"]
            
            print("Starting nodes...")
            for node_id in node_ids:
                cmd = [sys.executable, os.path.join(script_dir, "node.py"), "--id", node_id]
                if args.quiet:
                    cmd.append("--quiet")
                p = subprocess.Popen(cmd, cwd=script_dir)
                processes.append(p)
                print(f"  Started {node_id} (PID: {p.pid})")
                time.sleep(0.1)
            
            print("Waiting for nodes to initialize...")
            time.sleep(2)
        
        if not args.nodes_only:
            print(f"Starting controller with {args.csv}...")
            csv_path = args.csv if os.path.isabs(args.csv) else os.path.join(script_dir, args.csv)
            cmd = [sys.executable, os.path.join(script_dir, "controller.py"), "--csv", csv_path]
            
            if args.run_sets:
                cmd.extend(["--set", args.run_sets])
            if args.skip_sets:
                cmd.extend(["--skip", args.skip_sets])
            
            p = subprocess.Popen(cmd, cwd=script_dir)
            processes.append(p)
            print(f"  Started controller (PID: {p.pid})")
            
            p.wait()
        else:
            print("Nodes running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
    
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()

if __name__ == "__main__":
    main()