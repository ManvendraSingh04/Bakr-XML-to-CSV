# Bakr-XML-to-CSV
The project include the conversion from XML data to a CSV file and a self-ordering system.
The Pyhton script "new_article_import_Module1.py" take XML data from a website at 00:10 hrs and converted it into a CSV file.
The Python script "realtime_stock_info_Module2.py" take XML data from another website and compare that data with Module 1 data and converted it to a CSV file later.
Those scripts are uploaded to a FTP server after converstion.
The Python script "order_by_supplier_Module3.py" take data from CSV file that are uploaded realtime on FTP server and place order by using selenium.
