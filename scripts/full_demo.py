"""
full_demo.py

Purpose: Run both data writing and analysis in sequence.
"""
import subprocess

print("Generating sample data in all formats...")
subprocess.run(["python", "write_data.py"], cwd=".")

print("\nAnalyzing sample data in all formats...")
subprocess.run(["python", "analyze_data.py"], cwd=".")
