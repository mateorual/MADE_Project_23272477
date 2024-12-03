#!/bin/bash

# Description:
# This script automates the process of running tests for the project.
# It installs all required Python packages, executes the test suite, and reports the results.

# Prerequisites:
# 1. Ensure Python is installed and added to your PATH.
# 2. Run this script from the "project" directory or adjust the navigation step below.
# 3. Make sure the 'requirements.txt', 'tests.py', 'pipeline.py' and 'KMLExtractor_Helper.py' are in the project folder.

# Navigate to the project directory
cd "$(dirname "$0")"

# Step 1: Install required packages
echo "[INFO] Installing required Python packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Step 2: Run tests
echo "[INFO] Running tests..."
python tests.py

# Step 3: Check the test results
if [ $? -eq 0 ]; then
    echo "[INFO] All tests passed successfully!"
else
    echo "[ERROR] Some tests failed. Please check the output above for details."
    exit 1
fi


