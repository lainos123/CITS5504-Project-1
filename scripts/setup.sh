#!/bin/bash

# Sets up a Python virtual environment for the project if it doesn't exist

if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
else
  echo "Virtual environment already exists."
fi

source venv/bin/activate

pip3 install --upgrade pip
pip3 install -r requirements.txt

echo "Virtual environment ready! You can now run the ETL script "