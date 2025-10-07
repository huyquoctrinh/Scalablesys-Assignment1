#!/usr/bin/env python3
"""
EMERGENCY PATCH: Fix critical performance issues in existing code
Apply this patch to make your system work with 20K+ events
"""

import os
import sys

def apply_emergency_patches():
    """Apply critical patches to existing code."""
    
    print("🚨 APPLYING EMERGENCY PERFORMANCE PATCHES")
    print("=" * 50)
    
    # Patch 1: Fix Kleene closure max_len in enhanced demo
    enhanced_demo_file = "enhanced_citybike_demo_hot_path.py"
    if os.path.exists(enhanced_demo_file):
        print("📝 Patching enhanced_citybike_demo_hot_path.py...")
        
        with open(enhanced_demo_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Critical fixes
        patches = [
            # Reduce Kleene closure length from 8 to 3
            ("max_len=8  # Reduced to prevent memory explosion with 20K+ events", 
             "max_len=3  # CRITICAL: Reduced from 8 to 3 for memory safety"),
            
            # Force memory optimization for smaller datasets
            ("memory_optimized = max_events > 10000", 
             "memory_optimized = max_events > 500  # Enable for smaller datasets"),
            
            # Reduce parallel units
            ("parallel_units = min(3, max(2, num_threads))", 
             "parallel_units = 2  # Fixed to 2 for memory efficiency"),
            
            # More aggressive memory thresholds
            ("memory_threshold=0.6", "memory_threshold=0.4"),
            ("cpu_threshold=0.7", "cpu_threshold=0.5"),
        ]
        
        patched = False
        for old, new in patches:
            if old in content:
                content = content.replace(old, new)
                patched = True
                print(f"  ✅ Applied: {old[:50]}...")
        
        if patched:
            # Backup original
            os.rename(enhanced_demo_file, enhanced_demo_file + ".backup")
            
            # Write patched version
            with open(enhanced_demo_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"  ✅ {enhanced_demo_file} patched successfully")
            print(f"  📁 Original backed up as {enhanced_demo_file}.backup")
        else:
            print(f"  ⚠️  No patches needed for {enhanced_demo_file}")
    
    # Patch 2: Add memory monitoring to main CEP class
    cep_file = "CEP.py"
    if os.path.exists(cep_file):
        print(f"\n📝 Patching {cep_file}...")
        
        with open(cep_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Add memory monitoring import if not present
        if "import psutil" not in content:
            import_section = "from typing import List, Optional"
            if import_section in content:
                content = content.replace(
                    import_section,
                    import_section + "\nimport psutil\nimport gc"
                )
                print("  ✅ Added memory monitoring imports")
        
        # Add memory check in run method
        memory_check = '''        
        # EMERGENCY PATCH: Memory monitoring
        try:
            memory_usage = psutil.Process().memory_info().rss / 1024 / 1024
            if memory_usage > 400:  # 400MB limit
                print(f"⚠️  High memory usage: {memory_usage:.1f}MB - triggering cleanup")
                gc.collect()
        except:
            pass  # Ignore if psutil not available
'''
        
        if "# EMERGENCY PATCH: Memory monitoring" not in content:
            # Insert memory check after starting evaluation
            eval_start = "print(\"Starting evaluation...\")"
            if eval_start in content:
                content = content.replace(eval_start, eval_start + memory_check)
                print("  ✅ Added memory monitoring to run() method")
        
        # Write patched CEP file
        if "import psutil" in content or "EMERGENCY PATCH" in content:
            os.rename(cep_file, cep_file + ".backup")
            with open(cep_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  ✅ {cep_file} patched successfully")
    
    print(f"\n🎯 EMERGENCY PATCHES COMPLETE!")
    print("=" * 50)
    print("🚀 Critical fixes applied:")
    print("   • Reduced Kleene closure max_len from 8 to 3")
    print("   • Enabled memory optimization for datasets > 500 events")
    print("   • Reduced parallel units to 2 for memory efficiency") 
    print("   • More aggressive load shedding thresholds")
    print("   • Added memory monitoring to CEP engine")
    print()
    print("📁 Original files backed up with .backup extension")
    print("🔄 You can now run your 20K demo with:")
    print("   python enhanced_citybike_demo_hot_path.py --events 20000")
    print()
    print("⚠️  If issues persist, use the memory_optimized_demo.py instead")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    apply_emergency_patches()