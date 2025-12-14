# RBAC_IN_DATA_LAKES
This repository consists of our practical experiments for accessing files stored in an S3 data-bucket.

## How to run
To run the code AWS credentials are required. Ask Rasmus Herskind (rher) or Karl Kvaran (kkva) for the credentials.

Each experiment is designed to be able to run independent of each other.

## File structure 
```
RBAC_IN_DATA_LAKES/
│
├── imagesForReport/                # Figures and diagrams used in the report
│   ├── dalInformal.png              # Informal DAL architecture diagram
│   ├── ex01Informal.png             # Informal diagram – Experiment 1
│   ├── ex02Informal.png             # Informal diagram – Experiment 2
│   ├── ex03Informal.png             # Informal diagram – Experiment 3
│   ├── ex04Informal.png             # Informal diagram – Experiment 4
│   ├── rbac_in_data_lakes_experiments.drawio  # Editable Draw.io diagrams
│
├── src/                             # Source code
│   ├── data/                        # Data-related utilities
│   │
│   ├── AwsKmsClient.py              # AWS KMS wrapper for key access
│   ├── constants.py                 # Global constants (roles, keys, paths)
│   ├── DataAccessLayerClient.py     # DAL logic for encrypted read/write
│   ├── utility_functions.py         # Shared helper functions
│   │
│   ├── experiment1.ipynb            # Experiment 1 – Direct S3 access
│   ├── experiment2.ipynb            # Experiment 2 – Role-based decryption
│   ├── experiment3.ipynb            # Experiment 3 – DAL read simulation
│   ├── experiment4.ipynb            # Experiment 4 – DAL write simulation
│   ├── setup_bucket.ipynb           # S3 bucket and data setup
│
├── .env                             # Environment variables needed for AWS authentication (not committed) contact one of us to get them.

