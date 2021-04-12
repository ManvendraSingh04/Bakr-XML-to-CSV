# Importing the required libraries
import ftplib
import xml.etree.ElementTree as Xet
import pandas as pd
import requests
from datetime import datetime
from configparser import ConfigParser
import logging
import os

cur_dir = os.getcwd()
file_path = os.path.join(cur_dir, "log_files")
#Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(os.path.join(
    file_path, 'new_article_import_Module1_log.txt'), encoding="utf-8")
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s : %(message)s', datefmt=' %Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.info("Initiating new_article_import_Module1 Script ")

cols = ["StoItem Id", "Code", "Code2", "PartNo",  "PartNo2", "EAN", "EAN2", "Name", "NameE", "Match Code", "Hersteller", "ManName", "CouCode", "PriceDea", "Einstandspreis", "VK2-Preis", "UVP", "PriceRef", "PriceRef2", "Steuersatz", "TaxRate", "CutCode",
        "QtyFree", "WarDur", "WarDurEU", "Weight", "ScaId", "ThumbnailIs", "ThumbnailSize", "ImgIs", "ImgSize", "EnlargementIs", "EnlargementSize", "Note", "Picture 1", "Picture 2", "Picture 3", "Picture 4", "Picture 5", "Picture 6", "Picture 7", "up-to-date"]
rows = []

#Reading Data From config.ini
config_object = ConfigParser()
config_object.read('config.ini')
Module1URL = config_object['URLs']['Module1Url']
ImgGalBaseURL = config_object['URLs']['ImgGalBaseURL']
Picture1URL = config_object['URLs']['Picture1Url']
Picture1AltURL = config_object['URLs']['Picture1ALTUrl']
# FTP server credentials
FTP_HOST = config_object['FTP_Credentials']['FTP_HOST']
FTP_USER = config_object['FTP_Credentials']['FTP_USER']
FTP_PASS = config_object['FTP_Credentials']['FTP_PASS']
#FTP server connect for getting "For_Einstandspreis_Calculation" file
ftp_server = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
path = os.path.join(cur_dir, 'tmp_files')
file_path = os.path.join(path, "For_Einstandspreis_Calculation.csv")
filename = "For_Einstandspreis_Calculation.csv"
ftp_server.cwd("calculation-parameter-settings")
files = [(filename, file_path)]
for file_ in files:
    with open(file_[1], "wb") as f:
        ftp_server.retrbinary("RETR " + file_[0], f.write)
ftp_server.quit()
#Method for updation of column name "Einstandspreis"
def update_Einstandspreis(num):
    try:
        df1 = pd.read_csv(file_path, sep=';')
        for i in range(0, df1.shape[0]):
            price_from = df1['price_from'].astype(float)[i]
            from_to = df1["from_to"].astype(float)[i]
            rate = df1['%_Rate'].astype(float)[i]
            if num >= price_from and num <= from_to:
                num = (num*rate)/100+num
                break
        return num
    except Exception as ex:
        print(ex)
#Parsing the XML file from web
page = requests.get(Module1URL)
if page.status_code == 200:
   xmlparse = Xet.fromstringlist(page.text)
   logger.info("Getting data from Website for new_article_import")
   for i in xmlparse:
       Id = i.attrib.get("Id")
       Code = i.attrib.get("Code")
       Code_2 = i.attrib.get("Code2")
       PartNo = i.attrib.get("PartNo")
       PartNo2 = i.attrib.get("PartNo2")
       EAN = i.attrib.get("EAN")
       EAN2 = i.attrib.get("EAN2")
       Name = i.attrib.get("Name")
       NameE = i.attrib.get("NameE")
       Match_code = "BKR"
       Hersteller = "BKR"
       ManName = i.attrib.get("ManName")
       CouCode = i.attrib.get("CouCode")
       PriceDea = i.attrib.get("PriceDea")
       Einstandspreis = update_Einstandspreis(float(PriceDea))
       VK2_Preis = int((float(PriceDea) + 3.3)*(1.19)*(1.45))+0.99
       UVP = int(float(VK2_Preis) * 1.4)+0.99
       PriceRef = i.attrib.get("PriceRef")
       PriceRef2 = i.attrib.get("PriceRef2")
       Steuersatz = 19
       TaxRate = i.attrib.get("TaxRate")
       CutCode = i.attrib.get("CutCode")
       QtyFree = i.attrib.get("QtyFree")
       if QtyFree == None:
           QtyFree = "0"
       WarDur = i.attrib.get("WarDur")
       WarDurEU = i.attrib.get("WarDurEU")
       Weight = i.attrib.get("Weight")
       ScaId = i.attrib.get("ScaId")
       ThumbnailIs = i.attrib.get("ThumbnailIs")
       ThumbnailSize = i.attrib.get("ThumbnailSize")
       ImgIs = i.attrib.get("ImgIs")
       ImgSize = i.attrib.get("ImgSize")
       EnlargementIs = i.attrib.get("EnlargementIs")
       EnlargementSize = i.attrib.get("EnlargementSize")
       Note = i.attrib.get("Note")
       Picture1 = None
       if EnlargementIs == "1":
        Picture1 = Picture1URL+Id
       elif EnlargementIs == None and ImgIs == "1":
        Picture1 = Picture1AltURL+Id
       ImgGalTagval = [x.attrib.get("Tag") for x in i.findall("ImgGal")]
       ImgGalNameval = [x.attrib.get("Name") for x in i.findall("ImgGal")]
       ImgGalIdval = [x.attrib.get("Id") for x in i.findall("ImgGal")]
       Picture2 = Picture3 = Picture4 = Picture5 = Picture6 = Picture7 = None
       for j in range(0, len(ImgGalTagval)):
        if ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "1":
            Picture2 = ImgGalBaseURL + ImgGalIdval[j]
        elif ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "2":
            Picture3 = ImgGalBaseURL + ImgGalIdval[j]
        elif ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "3":
            Picture4 = ImgGalBaseURL + ImgGalIdval[j]
        elif ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "4":
            Picture5 = ImgGalBaseURL + ImgGalIdval[j]
        elif ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "5":
            Picture6 = ImgGalBaseURL + ImgGalIdval[j]
        elif ImgGalTagval[j] == "sys-gal-enl" and ImgGalNameval[j] == "6":
            Picture7 = ImgGalBaseURL + ImgGalIdval[j]
       date = datetime.now().strftime("%d.%m.%Y")
       rows.append({"StoItem Id": Id,
                        "Code": Code,
                        "Code2": Code_2,
                        "PartNo": PartNo,
                        "PartNo2": PartNo2,
                        "EAN": EAN,
                        "EAN2": EAN2,
                        "Name": Name,
                        "NameE": NameE,
                        "Match Code": Match_code,
                        "Hersteller": Hersteller,
                        "ManName": ManName,
                        "CouCode": CouCode,
                        "PriceDea": PriceDea,
                        "Einstandspreis": Einstandspreis,
                        "VK2-Preis": VK2_Preis,
                        "UVP": UVP,
                        "PriceRef": PriceRef,
                        "PriceRef2": PriceRef2,
                        "Steuersatz": Steuersatz,
                        "TaxRate": TaxRate,
                        "CutCode": CutCode,
                        "QtyFree": QtyFree,
                        "WarDur": WarDur,
                        "WarDurEU": WarDurEU,
                        "Weight": Weight,
                        "ScaId": ScaId,
                        "ThumbnailIs": ThumbnailIs,
                        "ThumbnailSize": ThumbnailSize,
                        "ImgIs": ImgIs,
                        "ImgSize": ImgSize,
                        "EnlargementIs": EnlargementIs,
                        "EnlargementSize": EnlargementSize,
                        "Note": Note,
                        "Picture 1": Picture1,
                        "Picture 2": Picture2,
                        "Picture 3": Picture3,
                        "Picture 4": Picture4,
                        "Picture 5": Picture5,
                        "Picture 6": Picture6,
                        "Picture 7": Picture7,
                        "up-to-date": date
                        })
else:
    logger.warning("Unsupported Hour data will be updated on next day at 00:10 hrs")
    print("Unsupported Hour data will be updated on next day at 00:10 hrs")
    exit()
df = pd.DataFrame(rows, columns=cols)
csv_path = os.path.join(cur_dir, "tmp_files")
csv_filename = os.path.join(csv_path, 'new-article-import.csv')
df.to_csv(csv_filename, index=False, sep=';')
logger.info("CSV file created sucessfully")
# connect to the FTP server
ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
# force UTF-8 encoding
ftp.encoding = "utf-8"
# local file name you want to upload
logger.info("Logging in to FTP server")
filename = "new-article-import.csv"
with open(csv_filename, "rb") as file:
    wdir = ftp.pwd()
    ftp.cwd('new-article-import')
    # use FTP's STOR command to upload the file
    ftp.storbinary(f"STOR {filename}", file)
    logger.info("File inserted sucessfully on FTP")
# list current files & directories
ftp.dir()
# quit and close the connection
ftp.quit()
logger.info("new_article_import_Module1 Script Ends")
