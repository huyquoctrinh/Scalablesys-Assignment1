#!/usr/bin/env python3
import re

# Read the corrupted file
with open('loadshedding/PersistentStatefulLoadShedding.py', 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

# Remove null bytes and other problematic characters
clean_content = content.replace('\x00', '').replace('\ufeff', '')

# Write back as clean UTF-8
with open('loadshedding/PersistentStatefulLoadShedding.py', 'w', encoding='utf-8') as f:
    f.write(clean_content)

print("File cleaned successfully")