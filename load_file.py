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
            "EndAttach" : None,
            "AttachRange" : None,
            "ParentBates" : None,
            "ChildBates" : None,
            "DocId" : None,
            "Custodian" : None,
            "From" : None,
            "To" : None,
            "CC" : None,
            "BCC" : None,
            "Subject" : None,
            "FileName" : None,
            "TimeZone" : None,
            "DateSent" : None,
            "TimeSent" : None,
            "DateReceived" : None,
            "TimeReceived" : None,
            "TimeSentTimeZone" : None,
            "MimeType" : None,
            "FileExt" : None,
            "Author" : None,
            "LastAuthor" : None,
            "DateCreated" : None,
            "TimeCreated" : None,
            "TimeCreatedTimeZone" : None,
            "DateModified" : None,
            "TimeModified" : None,
            "PrintedDate" : None,
            "PrintedTime" : None,
            "FileSize" : None,
            "PageCount" : None,
            "FilePath" : None,
            "MessageId" : None,
            "EmailHeader" : None,
            "EntryId" : None,
            "MD5Hash" : None,
            "SHA1Hash" : None,
            "SHA256Hash" : None,
            "NativeLink" : None,
            "OCRLink" : None
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
        "BegDoc": ["begdoc", "begbates", "beginbates", "batesbegin", "batestart", "startbates","firstbates"],
        "EndDoc": ["enddoc", "endbates", "batesend","lastbates"],
        "BegAttach": ["begattach"],
        "EndAttach": ["endattach"],
        "AttachRange" : ["attachrange"],
        "ParentBates" : ["parentbates"],
        "ChildBates" : ["childbates"],
        "DocId" : ["docid", "doc id", "doc_id"],
        "Custodian" : ["custodian"],
        "From" : ["from", "email from"],
        "To" : ["to", "email to"],
        "CC" : ["cc", "email cc"],
        "BCC" : ["bcc", "email bcc"],
        "Subject" : ["subject"],
        "FileName" : ["filename", "file name", "filename", "file"],
        "TimeZone" : ["timezone", "time zone", "time_zone"],
        "DateSent" : ["datesent", "date sent", "sentdate", "sent date"],
        "TimeSent" : ["timesent", "time sent", "senttime", "sent time"],
        "TimeSentTimeZone" : ["time_sent/time_zone",],
        "MimeType" : ["mimetype", "mime type", "mime_type"],
        "FileExt" : ["fileext", "file ext", "file extension", "file_extension", "file_ext"],
        "Author" : ["author"],
        "LastAuthor" : ["lastauthor", "last_saved_by", "last saved by", "savedby", "saved_by", "saved by"],
        "DateCreated" : ["datecreated", "date created", "createddate", "created date", "created_date", "createdate", "create_date", "create date"],
        "TimeCreated" : ["timecreated", "time created", "createdtime", "created time", "created_time", "createtime", "create_time", "create time"],
        "TimeCreatedTimeZone" : ["time_created/time_zone",],
        "DateModified" : ["datemodified", "date modified", "modifieddate", "modified date", "modified_date", "last modified", "last_modified", "lastmodified"],
        "TimeModified" : ["timemodified", "time modified", "modifiedtime", "modified time", "modified_time", "lastmodifiedtime", "last modified time"],
        "PrintedDate" : ["printeddate", "printed date", "printdate", "print date", "print_date"],
        "PrintedTime" : ["printedtime", "printed time", "printtime", "print time", "print_time"],
        "FileSize" : ["filesize", "file size", "file_size", "size"],
        "PageCount" : ["pagecount", "page count", "page_count", "pages", "pgcount"],
        "FilePath" : ["filepath", "file path", "file_path", "path", "intfilepath"],
        "MessageId" : ["messageid", "message id", "message_id", "msg_id", "msg", "intmsgid"],
        "EmailHeader" : ["emailheader", "email header", "email_header", "header"],
        "EntryId" : ["entryid", "entry id", "entry_id"],
        "MD5Hash" : ["md5hash", "md5 hash", "md5_hash", "md5"],
        "SHA1Hash" : ["sha1hash", "sha1 hash", "sha1_hash", "sha1"],
        "SHA256Hash" : ["sha256hash", "sha256 hash", "sha256_hash", "sha256"],
        "NativeLink" : ["nativelink", "native link", "native_link", "link"],
        "OCRLink" : ["ocrlink", "ocr link", "ocr_link", "ocr_path", "ocrpath", "ocr path", "text_path", "textpath", "text path"],

    }