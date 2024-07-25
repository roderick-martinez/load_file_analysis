# Load File Analyzer

This Python project shows how Python and pandas can be used to automate common eDiscovery load file tasks.

## Features

- Loads a DAT file into a pandas dataframe.
- Identifies common field headers.
- Identifies and highlights gaps in Bates numbers.

## Requirements

- Python 3.x
- pandas

## Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/roderick-martinez/load-file-analyzer.git
   cd load-file-analyzer
   ```

2. Install the required packages:
   ```sh
   pip install -r requirements.txt
   ```

## Usage

Run the script with the following command:

```sh
python main.py --file_path <path_to_your_file> --encoding <file_encoding>
```
