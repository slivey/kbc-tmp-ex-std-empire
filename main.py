from __future__ import print_function

"__author__ = 'Jeff Huth'"
"__credits__ = 'GreatVines 2017, Twitter: @drinkdata'"

"""
Python 3 environment (unicode script fixes in place)
"""

from ftplib import FTP
import csv
import zipfile
import gzip
import io
import os
import sys
import requests
import logging
import re

import pip
pip.main(['install', '--disable-pip-version-check',
          '--no-cache-dir', 'dateparser'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'pygelf'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'boto'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'boto3'])

from keboola import docker
from pygelf import GelfTcpHandler
import pandas as pd
import boto3
import botocore
from datetime import datetime


# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()
fields = {"_some": {"structured": "data"}}
logger.addHandler(GelfTcpHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=os.getenv('KBC_LOGGER_PORT'),
    version="1.0",
    debug=False, **fields
))
# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])

# initialize application
logging.info("Initializing Docker. CWD: " + os.getcwd())
cfg = docker.Config('/data/')

# Access the supplied parameters
logging.info("Getting config params.")
params = cfg.get_parameters()
ftp_host = cfg.get_parameters()["ftp_host"]
ftp_user = cfg.get_parameters()["ftp_user"]
ftp_password = cfg.get_parameters()["#ftp_password"]
ftp_dir = cfg.get_parameters()["ftp_dir"]
zip_pattern = cfg.get_parameters()["zip_pattern"]
zip_regex = cfg.get_parameters()["zip_regex"]
file_regex = cfg.get_parameters()["file_regex"]
s3_client = cfg.get_parameters()["s3_client"]
s3_secret = cfg.get_parameters()["#s3_secret"]
s3_bucket = cfg.get_parameters()["s3_bucket"]
s3_folder = cfg.get_parameters()["s3_folder"]


if __name__ == "__main__":
    """
    Main execution script.
    """

    # ftp connect
    logging.info("Connecting to ftp: " + ftp_host)
    ftp = FTP(ftp_host)
    ftp.login(ftp_user, ftp_password)
    ftp.set_pasv(True)
    ftp.cwd(ftp_dir)

    # s3 client init
    s3 = boto3.client('s3')

    logging.info("Getting list of remote ftp zip files: " + zip_pattern)
    for z in ftp.nlst(zip_pattern):  # Get list of zip files on remote FTP
        zname = z
        zsize = ftp.size(z)
        zmodified = datetime.strptime(ftp.sendcmd(
            'MDTM ' + z)[4:18], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        zdttm = str(re.search(zip_regex, z).group(2))

        # Download zip file locally
        logging.info("Downloading ftp zip file: " + z)
        ftp.retrbinary("RETR " + z, open("/data/in/files/" + z, 'wb').write)
        zf = zipfile.ZipFile("/data/in/files/" + z)

        for f in zf.namelist():  # Get list of text files in zip file
            ftype = str(re.search(file_regex, f).group(1)).replace(
                ' ', '').replace('-', '').replace('_', '')
            gz_file = ftype + ".N" + zdttm + ".gz"

            logging.info("Reading in text file: " + f)
            df = pd.read_csv(filepath_or_buffer=zf.open(f),  # read in text file to data frame
                             sep='\t',
                             quoting=csv.QUOTE_ALL,
                             quotechar='"',
                             doublequote=True,
                             low_memory=False,
                             encoding='ISO-8859-1')

            logging.info("Writing out gzip csv file: " + gz_file)
            content = df.to_csv(path_or_buf="/data/out/files/" + gz_file,  # write out gzip csv
                                sep=',',
                                header=True,
                                index=False,
                                quoting=csv.QUOTE_ALL,
                                quotechar='"',
                                doublequote=True,
                                line_terminator='\n',
                                encoding='utf-8',
                                compression='gzip')

            # upload gzip csv to s3
            s3_file_path = s3_folder + "/" + gz_file
            s3_file_exists = False
            try:
                response = s3.head_object(
                    Bucket=s3_bucket, Key=s3_file_path)  # check if file exists
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    s3_file_exists = False
                    logging.info("File does not exist on s3: " + gz_file)
                else:
                    raise e
            else:
                s3_file_exists = True

            if s3_file_exists:
                logging.info("File exists on s3 (skip): " + gz_file)
            else:
                try:
                    # upload the gz file to s3
                    logging.info("Uploading file to s3: " + gz_file)
                    s3.upload_file("/data/out/files/" + gz_file,
                                   s3_bucket, s3_file_path)
                except:
                    logging.info("Error uploading file to s3: " + gz_file)
            try:
                os.remove("/data/out/files/" + gz_file)
                logging.info("Local file removed: " + z)
            except:
                logging.info("Error removing local file: " + gz_file)

        logging.info("Closing zip file: " + z)
        zf.close()

        # upload zip file to s3
        s3_file_path = s3_folder + "/" + z
        s3_file_exists = False
        try:
            response = s3.head_object(
                Bucket=s3_bucket, Key=s3_file_path)  # check if file exists
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                s3_file_exists = False
                logging.info("File does not exist on s3: " + z)
            else:
                raise e
        else:
            s3_file_exists = True

        if s3_file_exists:
            logging.info("File exists on s3 (skip): " + z)
            ftp.delete(z)  # remove the zip file from ftp
        else:
            try:
                logging.info("Uploading file to s3: " + z)
                s3.upload_file("/data/in/files/" + z, s3_bucket,
                               s3_file_path)  # upload zip file to s3

                logging.info("Deleting remote ftp file: " + z)
                ftp.delete(z)  # remove the zip file from ftp
            except:
                logging.info("Error uploading file to s3: " + z)
        try:
            os.remove("/data/in/files/" + z)
            logging.info("Local file removed: " + z)
        except:
            logging.info("Error removing local file: " + z)

        logging.info("Closing ftp.")
        ftp.close()

        logging.info("Script completed.")
