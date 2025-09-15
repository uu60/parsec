#!/usr/bin/env python3
"""
Python script to orchestrate background vs JIT benchmark tests.
This script replaces the C++ loop logic and calls individual test executables.
"""

import subprocess
import csv
import datetime
import sys
import os
import re
import signal
from typing import List, Tuple, Dict

# Global variable to track if we should exit
should_exit = False

def signal_handler(signum, frame):
    """Handle Ctrl+C signal"""
    global should_exit
    print("\nReceived interrupt signal. Cleaning up and exiting...")
    should_exit = True
    
    # Kill any remaining processes
    try:
        subprocess.run(["pkill", "-9", "-f", "benchmark_jit_single"], capture_output=True, timeout=2)
        subprocess.run(["pkill", "-9", "-f", "benchmark_background_single"], capture_output=True, timeout=2)
        subprocess.run(["pkill", "-9", "-f", "mpirun"], capture_output=True, timeout=2)
    except:
        pass
    
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def parse_result_line(line: str) -> Tuple[str, int, int, float, float, float]:
    """Parse the RESULT line from C++ output"""
    if line.startswith("RESULT:"):
        parts = line[7:].strip().split(',')
        if len(parts) >= 6:
            primitive = parts[0]
            num = int(parts[1])
            width = int(parts[2])
            time0 = float(parts[3])
            time1 = float(parts[4])
            avg_time = float(parts[5])
            return primitive, num, width, time0, time1, avg_time
    raise ValueError(f"Invalid result line: {line}")

def run_single_test(executable: str, primitive: str, num: int, width: int, build_dir: str, host: str, batch_size: int) -> Tuple[float, float, float]:
    """Run a single test and return (time0, time1, avg_time)"""
    cmd = ["mpirun", "--bind-to", "none", "-np", "3"]
    
    # Add host parameter if specified
    if host:
        cmd.extend(["-host", host])
    
    cmd.extend([
        f"{build_dir}/primitives/benchmark/{executable}",
        f"--primitive={primitive}",
        f"--num={num}",
        f"--width={width}",
        "--batch_size", str(batch_size)
    ])
    
    process = None
    try:
        # Start the process with stderr redirected to suppress error messages
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, 
                                 text=True, bufsize=1, universal_newlines=True,
                                 preexec_fn=os.setsid)  # Create new session to avoid killing parent
        
        result_data = None
        
        # Monitor output line by line in real-time
        while True:
            try:
                line = process.stdout.readline()
                if not line:
                    # Process finished or no more output
                    break
                
                # Check if we got the RESULT line
                if line.startswith("RESULT:"):
                    try:
                        _, _, _, time0, time1, avg_time = parse_result_line(line.strip())
                        result_data = (time0, time1, avg_time)
                        
                        # Immediately terminate the process after getting the result
                        # Use SIGTERM first, then SIGKILL if needed
                        try:
                            os.killpg(process.pid, 15)  # SIGTERM to process group
                            import time
                            time.sleep(0.1)  # Give it a moment to terminate gracefully
                            if process.poll() is None:  # Still running
                                os.killpg(process.pid, 9)  # SIGKILL to process group
                        except:
                            try:
                                process.terminate()
                                import time
                                time.sleep(0.1)
                                if process.poll() is None:
                                    process.kill()
                            except:
                                pass
                        break
                    except Exception as e:
                        print(f"Error parsing result line: {e}")
                        break
            except:
                break
        
        # Clean up any remaining processes
        try:
            subprocess.run(["pkill", "-f", f"{executable}.*{primitive}.*{num}.*{width}"], 
                         capture_output=True, timeout=1)
        except:
            pass
        
        if result_data is None:
            return None, None, None
        
        return result_data
        
    except Exception as e:
        print(f"Exception running {executable}: {e}")
        # Force kill on exception
        if process:
            try:
                os.killpg(process.pid, 9)
            except:
                try:
                    process.kill()
                except:
                    pass
        return None, None, None
    finally:
        # Final cleanup - be more specific to avoid killing parent process
        try:
            if process and process.pid:
                subprocess.run(["pkill", "-f", f"{executable}.*{primitive}"], 
                             capture_output=True, timeout=1)
        except:
            pass

def main():
    # Configuration
    build_dir = "../../build"  # Relative to script location
    
    # Test parameters
    primitives = ["<", "<=", "==", "!=", "mux", "sort"]
    nums = [1000, 10000, 100000]
    sort_nums = [500, 1000, 5000]  # Separate sizes for sort
    widths = [1, 2, 4, 8, 16, 32, 64]
    host = ""  # Default: no host specification (local)
    batch_size = 1000  # Default batch size
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        if "--help" in sys.argv or "-h" in sys.argv:
            print("Usage: python3 run_bg_vs_jit_benchmark.py [OPTIONS]")
            print("Options:")
            print("  --nums=1000,10000         Comma-separated list of data sizes for non-sort operations")
            print("  --sort_nums=500,1000      Comma-separated list of data sizes for sort operations")
            print("  --widths=8,16,32          Comma-separated list of bit widths")
            print("  --host=va:1,oh:1,ca:1     MPI host specification")
            print("  --batch_size=1000         Batch size for operations")
            print("")
            print("Examples:")
            print("  python3 run_bg_vs_jit_benchmark.py --nums=1000,5000 --widths=8,16")
            print("  python3 run_bg_vs_jit_benchmark.py --host=va:1,oh:1,ca:1 --batch_size=2000")
            print("  python3 run_bg_vs_jit_benchmark.py --nums=100 --host=localhost:3")
            print("")
            print("Generated command format:")
            print("  mpirun --bind-to none -np 3 [-host HOST] EXECUTABLE --primitive=X --num=Y --width=Z --batch_size BATCH")
            return
        
        for arg in sys.argv[1:]:
            if arg.startswith("--nums="):
                nums = [int(x.strip()) for x in arg[7:].split(',')]
            elif arg.startswith("--sort_nums="):
                sort_nums = [int(x.strip()) for x in arg[12:].split(',')]
            elif arg.startswith("--widths="):
                widths = [int(x.strip()) for x in arg[9:].split(',')]
            elif arg.startswith("--host="):
                host = arg[7:].strip()
            elif arg.startswith("--batch_size="):
                batch_size = int(arg[13:].strip())
    
    print(f"Running benchmark with:")
    print(f"  Non-sort operations data sizes: {nums}")
    print(f"  Sort operations data sizes: {sort_nums}")
    print(f"  Bit widths: {widths}")
    print(f"  Primitives: {primitives}")
    print(f"  Host: {host if host else 'localhost'}")
    print(f"  Batch size: {batch_size}")
    
    # Generate timestamp for output files
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Results storage
    results = []
    
    # Calculate total tests
    total_tests = 0
    for pmt in primitives:
        current_nums = sort_nums if pmt == "sort" else nums
        total_tests += len(current_nums) * len(widths)
    
    current_test = 0
    
    # Run tests for each primitive
    for primitive in primitives:
        if should_exit:
            break
            
        # Choose appropriate data sizes and check if we should skip
        current_nums = sort_nums if primitive == "sort" else nums
        
        # Skip if data size contains 0 (indicating to skip this type of test)
        if 0 in current_nums:
            print(f"\nSkipping primitive: {primitive} (data size contains 0)")
            continue
            
        print(f"\nTesting primitive: {primitive}")
        
        for num in current_nums:
            if should_exit:
                break
                
            for width in widths:
                if should_exit:
                    break
                    
                current_test += 1
                print(f"Progress: {current_test}/{total_tests} - Testing {primitive}, num={num}, width={width}")
                
                # Run JIT test
                jit_time0, jit_time1, jit_avg = run_single_test("benchmark_jit_single", primitive, num, width, build_dir, host, batch_size)
                if jit_time0 is None or should_exit:
                    if not should_exit:
                        print(f"  JIT test failed, skipping...")
                    continue
                
                # Run Background test
                bg_time0, bg_time1, bg_avg = run_single_test("benchmark_background_single", primitive, num, width, build_dir, host, batch_size)
                if bg_time0 is None or should_exit:
                    if not should_exit:
                        print(f"  Background test failed, skipping...")
                    continue
                
                # Store results
                result = {
                    'timestamp': timestamp,
                    'primitive': primitive,
                    'num': num,
                    'width': width,
                    'jit_time0': jit_time0,
                    'jit_time1': jit_time1,
                    'jit_avg': jit_avg,
                    'bg_time0': bg_time0,
                    'bg_time1': bg_time1,
                    'bg_avg': bg_avg,
                    'bg_vs_jit_ratio': bg_avg / jit_avg if jit_avg > 0 else 0,
                    'jit_vs_bg_speedup': jit_avg / bg_avg if bg_avg > 0 else 0
                }
                results.append(result)
                
                print(f"  JIT: {jit_avg:.2f}ms, Background: {bg_avg:.2f}ms, Ratio: {result['bg_vs_jit_ratio']:.4f}")
    
    # Write results to CSV
    if results:
        filename = f"benchmark_bg_vs_jit_python_{timestamp}.csv"
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = [
                'Timestamp', 'Primitive', 'NumElements', 'Width', 'BatchSize', 'Host',
                'JIT_Time0_ms', 'JIT_Time1_ms', 'AvgJitTime_ms',
                'BG_Time0_ms', 'BG_Time1_ms', 'AvgBackgroundTime_ms',
                'BackgroundVsJitRatio', 'JitVsBackgroundSpeedup'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                writer.writerow({
                    'Timestamp': result['timestamp'],
                    'Primitive': result['primitive'],
                    'NumElements': result['num'],
                    'Width': result['width'],
                    'BatchSize': batch_size,
                    'Host': host if host else 'localhost',
                    'JIT_Time0_ms': f"{result['jit_time0']:.2f}",
                    'JIT_Time1_ms': f"{result['jit_time1']:.2f}",
                    'AvgJitTime_ms': f"{result['jit_avg']:.2f}",
                    'BG_Time0_ms': f"{result['bg_time0']:.2f}",
                    'BG_Time1_ms': f"{result['bg_time1']:.2f}",
                    'AvgBackgroundTime_ms': f"{result['bg_avg']:.2f}",
                    'BackgroundVsJitRatio': f"{result['bg_vs_jit_ratio']:.4f}",
                    'JitVsBackgroundSpeedup': f"{result['jit_vs_bg_speedup']:.4f}"
                })
        
        print(f"\nBenchmark completed! Results saved to: {filename}")
        print(f"Total tests executed: {len(results)}")
        
        # Summary statistics
        if results:
            avg_jit = sum(r['jit_avg'] for r in results) / len(results)
            avg_bg = sum(r['bg_avg'] for r in results) / len(results)
            avg_ratio = sum(r['bg_vs_jit_ratio'] for r in results) / len(results)
            
            print(f"Average JIT time: {avg_jit:.2f}ms")
            print(f"Average Background time: {avg_bg:.2f}ms")
            print(f"Average Background/JIT ratio: {avg_ratio:.4f}")
    else:
        print("No successful tests completed.")

if __name__ == "__main__":
    main()
