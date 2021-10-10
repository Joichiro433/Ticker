#!/bin/bash
pip install -r requirements.txt
conda install -c conda-forge ta-lib -y
conda install -c conda-forge xgboost -y
conda install -c conda-forge lightgbm -y
conda install -c conda-forge catboost -y