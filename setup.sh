#!/bin/bash
set -e

echo "Starting environment setup..."

# Create virtual environment (optional but recommended)
python -m venv venv

# Activate virtual environment
source venv/Scripts/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

echo "Environment setup completed successfully."
