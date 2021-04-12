import ftplib
import pandas as pd
from xml.dom import minidom
import requests
import xml.etree.ElementTree as Xet
from datetime import date
import os
import logging
from configparser import ConfigParser

cur_dir = os.getcwd()
file_path = os.path.join(cur_dir, "log_files")
#Reading Data From config.ini
config_object = ConfigParser()
config_object.read('config.ini')
cols = ["StoItem Id", "Code", "Code2", "PartNo", "EAN", "EAN2", "QtyFree"]
rows = []
Module1Url = config_object['URLs']['module1url']
Module2Url = config_object['URLs']['module2url']
# FTP server credentials
FTP_HOST = config_object['FTP_Credentials']['ftp_host']
FTP_USER = config_object['FTP_Credentials']['ftp_user']
FTP_PASS = config_object['FTP_Credentials']['ftp_pass']
ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
#Gets or create a logger
logger = logging.getLogger()
#set log level
logger.setLevel(logging.INFO)
#define file handler and set formatter
file_handler = logging.FileHandler(os.path.join(
    file_path, 'realtime_Stock_info_Module2_log.txt'), encoding="utf-8")
formatter = logging.Formatter(
    '%(asctime)s : %(levelname)s : %(filename)s : %(message)s', datefmt=' %Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
#add file handle to logger
logger.addHandler(file_handler)
logger.info("Initiating realtime_Stock_info_Module2 Script")
#method to convert XML format from Module1 to Module2
def XML_format_conversion():
    page = requests.get(Module1Url)
    if page.status_code == 200:
        logger.info(
            "Getting data from Website to convert XML format from Module1 to Module2")
        parser = Xet.fromstringlist(page.text)
        root = minidom.Document()
        xml = root.createElement('Result')
        root.appendChild(xml)
        for i in parser:
            productChild = root.createElement('StoItem')
            id = i.attrib.get('Id')
            productChild.setAttribute('Id', id)
            Code = i.attrib.get('Code')
            productChild.setAttribute('Code', Code)
            Code2 = i.attrib.get('Code2')
            productChild.setAttribute('Code2', Code2)
            PartNo = i.attrib.get('PartNo')
            productChild.setAttribute('PartNo', PartNo)
            EAN = i.attrib.get('EAN')
            productChild.setAttribute('EAN', EAN)
            EAN2 = i.attrib.get('EAN2')
            productChild.setAttribute('EAN2', EAN2)
            QtyFree = i.attrib.get('QtyFree')
            productChild.setAttribute('QtyFree', QtyFree)
            Date = str(date.today())
            productChild.setAttribute('Date', Date)
            xml.appendChild(productChild)
            xml_str = root.toprettyxml(indent="\t")
        save_path_file = "M1convertedtoM2.xml"
        print("file created")
        xml_path = os.path.join(cur_dir, "tmp_files")
        with open(os.path.join(xml_path, save_path_file), "w") as f:
            f.write(xml_str)
        logger.info("XML Data is converted in desired format")
        file_creation_for_reatime_stock_info_Module2()
    else:
        logger.warning(
            "Unsupported Hour data will be updated on next day at 00:10 hrs")
        exit()
#method to create file realtime_stock_info.csv
def file_creation_for_reatime_stock_info_Module2():
    page = requests.get(Module2Url)
    xmlparse = Xet.fromstringlist(page.text)
    logger.info("Getting data from Website for realtime_stock_info")
    xml_path = os.path.join(cur_dir, 'tmp_files')
    newroot = Xet.parse(os.path.join(
        xml_path, "M1convertedtoM2.xml")).getroot()
    for new2 in newroot:
        id = new2.attrib.get("Id")
        Code = new2.attrib.get("Code")
        Code2 = new2.attrib.get("Code2")
        PartNo = new2.attrib.get("PartNo")
        EAN = new2.attrib.get("EAN")
        EAN2 = new2.attrib.get("EAN2")
        QtyFree = "0"
        for new in xmlparse:
            id2 = new.attrib.get("Id")
            if id == id2:
                QtyFree = new.attrib.get("QtyFree")
                break
            else:
                continue
        rows.append({"StoItem Id": id,
                     "Code": Code,
                     "Code2": Code2,
                     "PartNo": PartNo,
                     "EAN": EAN,
                     "EAN2": EAN2,
                     "QtyFree": QtyFree})
    df = pd.DataFrame(rows, columns=cols)
    csv_path = os.path.join(cur_dir, "tmp_files")
    csv_filename = os.path.join(csv_path, 'realtime-stock-info.csv')
    df.to_csv(csv_filename, sep=';')
    logger.info("CSV file created sucessfully")
    # local file name you want to upload
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    filename = "realtime-stock-info.csv"
    with open(csv_filename, "rb") as file:
        ftp.cwd('realtime-stock-info')
        ftp.storbinary(f"STOR {filename}", file)
        logger.info("File inserted sucessfully on FTP")
        ftp.quit()


xml_path = os.path.join(cur_dir, "tmp_files")
if os.path.exists(os.path.join(xml_path, 'M1convertedtoM2.xml')) == True:
    if Xet.parse(os.path.join(xml_path, 'M1convertedtoM2.xml')).getroot().find('StoItem').attrib.get('Date') == str(date.today()):
        file_creation_for_reatime_stock_info_Module2()
    else:
        logger.warning("Data is from different date")
        XML_format_conversion()
else:
    logger.warning("File not found to convert data ")
    XML_format_conversion()
