#!/usr/bin/env bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
echo "Virtual environment created and dependencies installed."
echo "To activate, run: source venv/bin/activate"
