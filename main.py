import argparse
from load_file import LoadFile

def main(file_path, encoding):
    lf = LoadFile(file_path, encoding)
    lf.read_load_file()
    lf.detect_columns()

    gaps = lf.detect_gaps()
    print(f"Note that this doesn't account for Bates numbers with suffixes.")
    if gaps is not None:
        print_red_text("\n\nGAPS FOUND.")
        print_red_text("See the records tagged with 'True' in the 'Gap' column. The record after the gap is also shown.")
        print(gaps)
    else:
        print("No gaps detected.")

def print_red_text(text):
    # ANSI codes
    RED = "\033[31m"
    RESET = "\033[0m"

    print(f"{RED}{text}{RESET}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", type=str, help="Path to load file to be analyzed")
    parser.add_argument("--encoding", type=str, help="Encoding of the file. Default is utf-8", default="utf-8")
    
    main(parser.parse_args().file_path, parser.parse_args().encoding)