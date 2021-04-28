import ftplib
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from configparser import ConfigParser 
import time
import schedule
from datetime import date,datetime
import logging
import os

#Reading Data From config.ini
config_object = ConfigParser()
config_object.read('config.ini')
#Gets or create a logger
logger = logging.getLogger()
#set log level
logger.setLevel(logging.INFO)
#define file handler and set formatter
file_handler = logging.FileHandler('order_by_supplier_log.txt')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s : %(message)s', datefmt=' %Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
#add file handle to logger
logger.addHandler(file_handler)
# FTP server credentials
FTP_HOST = config_object['FTP_Credentials']['ftp_host']
FTP_USER = config_object['FTP_Credentials']['ftp_user']
FTP_PASS = config_object['FTP_Credentials']['ftp_pass']
web_driver_Path = config_object['ChromeWebDriver']['Path']
Module3URL = config_object['URLs']['Module3Url']
Web_Username = config_object['order_by_brk_Credentials']['username']
Web_Password = config_object['order_by_brk_Credentials']['password']
ftp_server = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
logger.info("Initiating realtime_Stock_info_Module3 Script")

def chrome_driver(data):
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(options=options,executable_path=web_driver_Path)
    driver = webdriver.Chrome(executable_path=web_driver_Path)
    driver.get(Module3URL)
    user = driver.find_element_by_name("~ConLogName")
    pswd = driver.find_element_by_name("~ConLogPswd")
    user.clear()
    pswd.clear()
    user.send_keys(Web_Username)
    pswd.send_keys(Web_Password)
    logger.info("Logging into web to order items")
    login = driver.find_element_by_xpath('//*[@id="logoncont"]/form/div[2]/div[1]/a')
    login.click()
    driver.get(Module3URL)
    dataentry = driver.find_element_by_name("txtOrd")
    dataentry.clear()
    dataentry.send_keys(data)
    logger.info("Data Inserted Sucessfully")
    imprt = driver.find_element_by_xpath('//*[@id="centercont"]/form/input[8]')
    imprt.click()
    basket = driver.find_element_by_xpath('//*[@id="topmenu_X6"]/a')
    basket.click()
    Continue_order1 = driver.find_element_by_xpath('//*[@id="btnnOrd1Buy"]/a')
    Continue_order1.click()
    orderDate= driver.find_element_by_name("ordcodeo")
    orderDate.clear()
    orderDate.send_keys(datetime.now().strftime("%Y%m%d%H%M"))
    time.sleep(2)
    try:
        link_previous = driver.find_element_by_xpath('//*[@id="centercont"]/div[1]/div/form/div[3]/div[2]/div/table/tbody/tr[3]/td[1]/input[1]')
        link_previous.click()
    except Exception :
        logger.error("Exception occured",exc_info=True)
    Continue_order2 = driver.find_element_by_xpath('//*[@id="btnnOrd2Buy"]/a')
    Continue_order2.click()
    definite_order = driver.find_element_by_xpath('//*[@id="btnnOrd3Buy"]/a')
    definite_order.click()
    logger.info("Listed items ordered sucessfully")
    time.sleep(5)
    driver.quit()

def orders_by_brk():
    ftp_server = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    ftp_server.cwd("orders-by-bkr")
    All_files_on_FTP = ftp_server.nlst()  #To read all files from FTP
    logger.info('Getting requried files from FTP')
    files_needed_today = str(date.today())+"-"
    for file1 in All_files_on_FTP:
        if(file1.find("_No_Data_found_")==-1):             
            if (file1.startswith(files_needed_today) and file1.endswith('.csv')):
                if(file1.find("_Ordered_")==-1):
                    filename = file1
                    files = [(filename, 'newfile.csv')]
                    for file_ in files:
                        with open(file_[1], "wb") as f:
                            ftp_server.retrbinary("RETR " + file_[0], f.write)
                    df = pd.read_csv('newfile.csv', sep=';')
                    data = []
                    for i in range(0, df.shape[0]):
                        newdata = "I;"+str(df["Lagerhaltungsnummer"].values[i]) + \
                            ";"+str(df["Anzahl"].values[i])+"\n"
                        data.append(newdata)
                    if not data:
                        logger.info("No data found in file : "+filename)
                        base = os.path.splitext(filename)[0]
                        newfilename = base+"_No_Data_found_"+datetime.now().strftime("%Y%m%d%H%M")+".csv"
                        ftp_server.rename(filename, newfilename)
                        continue
                    logger.info("Extracted data is ready to upload on web from file : "+filename)
                    chrome_driver(data)
                    base = os.path.splitext(filename)[0]
                    newfilename = base+"_Ordered_"+datetime.now().strftime("%Y%m%d%H%M")+".csv"
                    ftp_server.rename(filename, newfilename)
                    logger.info("File name changed after successful order")
            elif not (file1.startswith(files_needed_today) and file1.endswith('.csv')):
                continue
        elif(file1.find("_Ordered_")==-1):
            if (file1.startswith(files_needed_today) and file1.endswith('.csv')):
                if(file1.find("_No_Data_found_")==-1):
                    filename = file1
                    files = [(filename, 'newfile.csv')]
                    for file_ in files:
                        with open(file_[1], "wb") as f:
                            ftp_server.retrbinary("RETR " + file_[0], f.write)
                    df = pd.read_csv('newfile.csv', sep=';')
                    data = []
                    for i in range(0, df.shape[0]):
                        newdata = "I;"+str(df["Lagerhaltungsnummer"].values[i]) + \
                            ";"+str(df["Anzahl"].values[i])+"\n"
                        data.append(newdata)
                    if not data:
                        logger.info("No data found in file : "+filename)
                        base = os.path.splitext(filename)[0]
                        newfilename = base+"_No_Data_found_"+datetime.now().strftime("%Y%m%d%H%M")+".csv"
                        ftp_server.rename(filename, newfilename)
                        continue
                    logger.info("Extracted data is ready to upload on web from file : "+filename)
                    chrome_driver(data)
                    base = os.path.splitext(filename)[0]
                    newfilename = base+"_Ordered_"+datetime.now().strftime("%Y%m%d%H%M")+".csv"
                    ftp_server.rename(filename, newfilename)
                    logger.info("File name changed after successful order")
            elif not (file1.startswith(files_needed_today) and file1.endswith('.csv')):
                continue
        else:
            continue
    logger.info("No new file found")
    return

def job():
    if datetime.now().hour >=5 and datetime.now().hour<=21:
        orders_by_brk()
    else:
        ftp_server.close()
        os.remove('newfile.csv')
        logger.warning("Unsupported hour please try after 05:00 hrs")
        schedule.CancelJob
        exit()

schedule.every(15).minutes.do(job)                  #To change schedule time 

while True:
    schedule.run_pending()
    ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    time.sleep(1)

