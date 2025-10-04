#!/usr/bin/env python3
"""
Standalone evaluation runner for load shedding experiments.
Usage: python evaluation_runner.py --csv data.csv --events 5000
"""

import os
import sys
import argparse
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from loadshedding.evaluator import LoadSheddingEvaluator


def main():
    parser = argparse.ArgumentParser(
        description='Run load shedding evaluation experiments'
    )
    parser.add_argument('--csv', required=True,
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=5000,
                       help='Maximum events to process')
    parser.add_argument('--output', default='evaluation_results.txt',
                       help='Output file for results')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv):
        print(f"Error: File not found: {args.csv}")
        sys.exit(1)
    
    print("="*60)
    print("LOAD SHEDDING EVALUATION")
    print("="*60)
    print(f"Start time: {datetime.now()}")
    print(f"CSV file: {args.csv}")
    print(f"Max events: {args.events}")
    print()
    
    # Run evaluation
    evaluator = LoadSheddingEvaluator(args.csv, args.events)
    results = evaluator.run_full_evaluation()
    
    # Save results
    with open(args.output, 'w') as f:
        f.write(f"Load Shedding Evaluation Results\n")
        f.write(f"Timestamp: {datetime.now()}\n")
        f.write(f"CSV: {args.csv}\n")
        f.write(f"Events: {args.events}\n\n")
        
        for result in results:
            f.write(f"\n{'='*50}\n")
            f.write(f"Config: {result['config']}\n")
            f.write(f"Duration: {result['duration']:.2f}s\n")
            f.write(f"Matches: {result['matches_found']}\n")
            f.write(f"Recall: {result['recall']:.2%}\n")
            if result['load_shedding']:
                f.write(f"Drop Rate: {result['load_shedding']['drop_rate']:.2%}\n")
    
    print(f"\n✓ Results saved to: {args.output}")
    print(f"End time: {datetime.now()}")


if __name__ == "__main__":
    main()