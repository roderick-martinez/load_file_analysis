import itertools
import pandas as pd

class LoadFile:
    def __init__ (self, file_path, encoding="utf-8"):
        self.file_path = file_path
        self.encoding = encoding
        self.field_delimiter = "\x14"
        self.quote_char = "\xfe"
        self.newline_char = "\xae"
        self.dataframe = None
        
        # Dictionary to store standard fields and their corresponding columns in the dataframe
        self.standard_fields = {
            "BegDoc" : None,
            "EndDoc" : None,
            "BegAttach" : None,
            "EndAttach" : None
        }

    def read_load_file(self):
        try:
            self.dataframe = pd.read_csv(
                self.file_path,
                delimiter=self.field_delimiter,
                quotechar=self.quote_char,
                encoding=self.encoding,
                engine="python") # needed to handle DAT delimiters
        except Exception as e:
            print(f"Error reading file: {e}")
    
    def detect_columns(self):
        """
        Detects the columns in the dataframe that correspond to the standard fields.
        The standard fields and known names are defined in the KNOWN_COLUMN_NAMES dictionary.
        The matched columns are stored in the standard_fields dictionary.
        """
        for standard_field, known_names in self.KNOWN_COLUMN_NAMES.items():
            for column in self.dataframe.columns:
                if column.lower() in known_names:
                    self.standard_fields[standard_field] = column
                    break
    
    def detect_gaps(self):
        if self.standard_fields["BegDoc"] is None or self.standard_fields["EndDoc"] is None:
            print("ERROR: BegDoc and EndDoc fields are required to detect gaps.")
            return
        else:
            # Strip the text portion of the Bates number and copy to a new int column
            self.dataframe["BegDocNumeric"] = self.dataframe[self.standard_fields["BegDoc"]].str.extract(r"(\d+)").astype(int)
            self.dataframe["EndDocNumeric"] = self.dataframe[self.standard_fields["EndDoc"]].str.extract(r"(\d+)").astype(int)
            self.dataframe = self.dataframe.sort_values(by="BegDocNumeric", ascending=True)
            self.dataframe["Gap"] = (self.dataframe["BegDocNumeric"].shift(-1) - self.dataframe["EndDocNumeric"]) != 1
            # self last row to false; it gets incorrectly marked as a gap
            self.dataframe.loc[self.dataframe.index[-1], "Gap"] = False

            # find the records and one after the gap
            gap_indices = self.dataframe.index[self.dataframe["Gap"]].tolist()
            gaps_plus_after = list(map(lambda idx: [idx, idx+1], gap_indices))
            # flatten the list
            gaps_plus_after = list(itertools.chain(*gaps_plus_after))

            if gap_indices:
                return self.dataframe.loc[gaps_plus_after]
            else:
                return None

        
    # Edit this to update autodetection
    KNOWN_COLUMN_NAMES = {
        "BegDoc": ["begdoc", "begbates", "beginbates", "batesbegin", "batestart", "startbates"],
        "EndDoc": ["enddoc", "endbates", "batesend"],
        "BegAttach": ["begattach"],
        "EndAttach": ["endattach"]
    }