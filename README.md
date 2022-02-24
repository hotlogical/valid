# Data Validation Prototype



Part of the mini-POC. Scope...

- Generate Caspian data meta-schema
- Ingest a structured dataset with arrow (csv, json, xml etc.)
- Save the dataset directly to parquet
- Run Data Schema Generation tool
    - Load parquet metadata
    - Build table of column metadata
    - Calculate and display column metrics
    - Generate and display interactive graph of column data
    - Generate schema input form from caspian meta-schema
    - Capture constraints, transforms, standardisations.
    - Capture Table level metadata and constraints
- Validata dataset schema against meta-schema
- Store dataset schema.


[Installation](#installation)
## Installation

Miniconda is the preferred python version, download from

https://docs.conda.io/en/latest/miniconda.html

```
sha256sum Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
conda update conda
python --version
```

It's much faster to install required packages using mamba (a c++ rewrite of conda)
```
conda install mamba -c conda-forge
```
Create a conda environment called 'valid' and install packages
```
mamba create -n valid -c conda-forge root pandas pyarrow jupyterlab jupytext matplotlib seaborn plotly streamlit pydantic
conda activate valid
```

To run the Data Schema Generation tool, from a terminal
```
streamlit run appwiz.py 
```

Then the app will be served from localhost:8501

The port can be changed by adding --port to the command line.