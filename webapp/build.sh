#!/usr/bin/env bash
set -e
pip install -r webapp/requirements.txt
cd webapp && python load_data.py
