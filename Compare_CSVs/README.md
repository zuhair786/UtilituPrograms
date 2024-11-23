# CSV Comparison Script

## Overview

This Python script compares two CSV files, finds common rows based on the first column, and writes the common rows into a result file. It also supports writing additional data from a queue to the output file. The script is useful for quick comparison and filtering tasks involving CSV files.

---

## Features

- **Read CSV Files:** Reads and parses CSV files into lists of rows.
- **Compare CSV Files:** Finds rows with common keys in the first column from two input files.
- **Write Results:** Outputs the common rows into a new CSV file.
- **Queue Integration:** Supports appending data from a `queue.Queue` to the output file.

---

## Prerequisites

- Python 3.x
- `csv` module (standard library)
- `os` module (standard library)
- `queue` module (standard library)

---

## Usage

1. **Input Files:**
   - Place your source files (`source1.txt` and `source2.txt`) in the same directory as the script. 
   - Ensure these files are formatted as valid CSV files.

2. **Output File:**
   - The resulting common rows will be written to a file named `result.txt` in the same directory.

3. **Run the Script:**
   Execute the script using:
   ```bash
   python script_name.py
