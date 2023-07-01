from oauth2client.service_account import ServiceAccountCredentials
import gspread


class GSheetWriter:
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    CREDENTIALS = "credentials.json"
    SHEET_NAME = "RBorrow Study"

    def __init__(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CREDENTIALS, self.SCOPES)
        file = gspread.authorize(credentials)
        self.sheet = file.open(self.SHEET_NAME)
        self.worksheet = self.sheet.worksheet("rBorrow RAW")

    def write(self):
        self.worksheet.update('A2:B3', [["Not Ford", "Not Lancia"], ["Nothing", "Not"]])


GSheetWriter().write()
