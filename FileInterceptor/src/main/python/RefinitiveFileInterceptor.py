import json
import os
from builtins import print
from json import dumps
from pathlib import Path
import stomp
import sys
import time
import datetime
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, date
from kafka import KafkaProducer

# default_args = {
#     'owner': 'DevOps',
#     'depends_on_past': False,
#     'email': ['Irfan.Ahemad@smartstreamrdu.com'],
#     'email_on_failure': True,
#     'email_on_retry': False,
#     #        'retries': 0,
#     #        'retry_delay': timedelta(minutes = 5),
# }
#
# dag = DAG('RFI_TASK', default_args=default_args, schedule_interval="*/15 * * * *", start_date=datetime(2022, 11, 11),
#           catchup=False, tags=['RFI'])

dependencyLog = []

if len(sys.argv) != 2:
    sys.exit("Please provide config file path")
else:
    print(f'Config file is {sys.argv[1]}')

# Loading Config
with open(sys.argv[1], "r") as f:
    config = json.load(f)
    # Reading File locations
    camel_download_location = Path(config["camel_download_location"])
    sent_files_location = Path(config["sent_files_location"])
    Waiting_files_location = Path(config["Waiting_files_location"])
    download_filter = filter(lambda x: os.path.isfile(os.path.join(camel_download_location, x)),
                             os.listdir(camel_download_location))
    download_files = sorted(download_filter, key=lambda x: os.path.getmtime(os.path.join(camel_download_location, x)))
    sent_filter = filter(lambda x: os.path.isfile(os.path.join(sent_files_location, x)),
                         os.listdir(sent_files_location))
    sent_files = sorted(sent_filter, key=lambda x: os.path.getmtime(os.path.join(sent_files_location, x)))
    waiting_filter = filter(lambda x: os.path.isfile(os.path.join(Waiting_files_location, x)),
                            os.listdir(Waiting_files_location))
    Waiting_files = sorted(waiting_filter, key=lambda x: os.path.getmtime(os.path.join(Waiting_files_location, x)))
    dependencies = config["dependencies"]
    em_file_index = config["FileNameIndex"]["EM"]
    refinitive_file_index = config["FileNameIndex"]["refinitive"]
    MessageProducerConfig = config["MessageProducerConfig"]
    LOG = config["Logging"]


def filterFile(fileLocation, filename):
    flag = False
    for filterInfo in config["filterFiles"]:
        if bool(re.match(str(filterInfo['regex']), filename)):
            if LOG["logFilterFiles"]:
                print("Filtering File " + str(filename) + " , Regex ID - " + str(filterInfo['id']))
                flag = True
            if filterInfo['moveFileTo']:
                moveFile(fileLocation / filename, Path(filterInfo['moveFileTo']), filename)
                if LOG["logFilterFiles"]:
                    print("Moving File to  " + str(filterInfo['moveFileTo']))
            break
    return flag


def sortedReasons():
    seen = set()
    seen_add = seen.add
    return [x for x in dependencyLog if not (x in seen or seen_add(x))]


def printDependencySummery(flag, msg):
    reasons = sortedReasons()
    global dependencyLog
    dependencyLog = []
    summery = ''
    for reason in reasons:
        summery = summery + str(reason) + ' '
    if flag:
        print(summery)
        print(msg)


def isEM(file_path):
    return file_path.stem[em_file_index["em"]:em_file_index["em"] + 2] == 'EM'


def isEMFile(file_path):
    flag = file_path.stem[em_file_index["em"]:em_file_index["em"] + 2] == 'EM'
    if flag:
        dependencyLog.append(str(file_path.name) + " is an EM file ,")
    else:
        dependencyLog.append(str(file_path.name) + " is a NOT an EM file ,")
    return flag


def getFileCode(file_path):
    if isEM(file_path):
        fileCode = file_path.stem[em_file_index["file_code"]:em_file_index["file_code"] + 4]
    else:
        fileCode = file_path.stem[refinitive_file_index["file_code"]:refinitive_file_index["file_code"] + 4]
    return fileCode


def getCompleteEMFileDate(file_path):
    mm = int(file_path.stem[em_file_index["file_month"]:em_file_index["file_month"] + 2])
    dd = int(file_path.stem[em_file_index["file_date"]:em_file_index["file_date"] + 2])
    yyyy = int(file_path.stem[em_file_index["file_year"]:em_file_index["file_year"] + 4]) ##New    
    pivot_date = date(yyyy, mm, dd) ##New
    return pivot_date


def getFileDate(file_path):
    if isEM(file_path):
        mm = int(file_path.stem[em_file_index["month"]:em_file_index["month"] + 2])
        dd = int(file_path.stem[em_file_index["date"]:em_file_index["date"] + 2])
        yyyy = int(file_path.stem[em_file_index["year"]:em_file_index["year"] + 4]) ##New
    else:
        mm = int(file_path.stem[refinitive_file_index["month"]:refinitive_file_index["month"] + 2])
        dd = int(file_path.stem[refinitive_file_index["date"]:refinitive_file_index["date"] + 2])
        yyyy = int(file_path.stem[refinitive_file_index["year"]:refinitive_file_index["year"] + 4]) #New

    pivot_date = date(yyyy, mm, dd)
    return pivot_date


def getEMFileNo(file_path):
    return int(file_path.stem[em_file_index["fileNo"]:em_file_index["fileNo"] + 1])


def getEMRegionNo(file_path):
    return int(file_path.stem[em_file_index["region"]:em_file_index["region"] + 1])


def notFirstEMFileNo(file_path):
    flag = (0 != getEMFileNo(file_path))
    if flag:
        dependencyLog.append(str(file_path.name) + " is not a first EM file ,")
    else:
        dependencyLog.append(str(file_path.name) + " is a first EM file ,")
    return flag


def is_saturday(file_path):
    file_date = getFileDate(file_path)
    flag = file_date.weekday() == 5
    if flag:
        dependencyLog.append(str(file_path.name) + " is a saturday file")
    else:
        dependencyLog.append(str(file_path.name) + " is not a saturday file")
    return flag


def is_Friday_Maintenance_Sent(file_path):
    fileCode = getFileCode(file_path)
    I_file_date = getFileDate(file_path)
    M_file_date = I_file_date - timedelta(days=1)
    M_file_dateStamp = M_file_date.strftime("%Y%m%d")
    sentFileM = fileCode + M_file_dateStamp + ".M"
    sentFileW = fileCode + M_file_dateStamp + ".W"

    requiredSentFileM = sent_files_location / sentFileM
    requiredSentFileW = sent_files_location / sentFileW
    flag = requiredSentFileM.exists() or requiredSentFileW.exists()

    if flag:
        dependencyLog.append(" And " + str(requiredSentFileM.name) + "|W" + " file is sent. ( FRIDAY M ) ")
    else:
        dependencyLog.append(" And " + str(requiredSentFileM.name) + "|W" + " file is not yet sent.  ( FRIDAY M ) ")
    return flag


def is_Saturday_Full_Sent(file_path):
    fileCode = getFileCode(file_path)
    file_date = getFileDate(file_path)
    I_file_dateStamp = file_date.strftime("%Y%m%d")
    sentFileI = fileCode + I_file_dateStamp + ".I"
    sentFileV = fileCode + I_file_dateStamp + ".V"

    requiredSentFileI = sent_files_location / sentFileI
    requiredSentFileV = sent_files_location / sentFileV

    flag = requiredSentFileI.exists() or requiredSentFileV.exists()

    if flag:
        dependencyLog.append(" And " + str(requiredSentFileI.name) + "|V" + " file is sent. ( SATURDAY I ) ")
    else:
        dependencyLog.append(" And " + str(requiredSentFileI.name) + "|V" + str(
            requiredSentFileV.name) + " file is not yet sent.  ( SATURDAY I ) ")
    return flag


def isPrevEMFileNoSent(file_path):
    fileNo = getEMFileNo(file_path)
    fileNo = fileNo - 1
    sentFile = 'EM' + str(fileNo) + file_path.stem[3:] + file_path.suffix
    requiredSentFile = sent_files_location / sentFile
    flag = requiredSentFile.exists()

    if flag:
        dependencyLog.append(" And " + str(requiredSentFile.name) + " file is sent. ( Previous EM File ) ")
    else:
        dependencyLog.append(" And " + str(requiredSentFile.name) + " file is not yet sent.  ( Previous EM File ) ")
        requiredSentFile = Waiting_files_location / sentFile
        if requiredSentFile.exists():
            dependencyLog.append(" And " + str(requiredSentFile.name) + " is in waiting state")
        else:
            dependencyLog.append(" And " + str(requiredSentFile.name) + " is not in waiting state")
            requiredSentFile = sent_files_location / (sentFile.split('_')[0] + file_path.suffix)
            flag = requiredSentFile.exists()
            if flag:
                dependencyLog.append(
                    " But " + str(requiredSentFile.name) + " file is sent. ( Previous EM File Complete ) ")
            else:
                dependencyLog.append(" As Well as " + str(
                    requiredSentFile.name) + " file is not yet sent.  ( Previous EM File Complete ) ")

    return flag


def isYesterdaysFileSent(file_path):
    file_code = getFileCode(file_path)
    file_date = getFileDate(file_path)
    file_date_yesterday = file_date - timedelta(days=1)

    pivot_file_name = str(file_code) + str(file_date_yesterday.strftime("%Y%m%d")) + file_path.suffix
    is_em_file = isEM(file_path)
    if is_em_file:
        complete_file_date = getCompleteEMFileDate(file_path)
        complete_file_date_yesterday = complete_file_date - timedelta(days=1)

        pivot_file_name = 'EM' + str(getEMFileNo(file_path)) + str(getEMRegionNo(file_path)) + str(
            complete_file_date_yesterday.strftime("%Y%m%d")) + '_HO' + pivot_file_name

    yesterday_file_name = sent_files_location / pivot_file_name
    flag = yesterday_file_name.exists()
    if flag:
        dependencyLog.append(" And " + str(yesterday_file_name.name) + " file was sent. ( Yesterdays File ) ")
    else:
        dependencyLog.append(" And " + str(yesterday_file_name.name) + " file is not yet sent.  ( Yesterday File ) ")

        if is_em_file:
            requiredSentFile = Waiting_files_location / yesterday_file_name

            if requiredSentFile.exists():
                dependencyLog.append(" And " + str(requiredSentFile.name) + " is in waiting state")
            else:
                dependencyLog.append(" And " + str(requiredSentFile.name) + " is not in waiting state")
                requiredSentFile = sent_files_location / (pivot_file_name.split('_')[0] + file_path.suffix)
            flag = requiredSentFile.exists()
            if flag:
                dependencyLog.append(
                    " But " + str(requiredSentFile.name) + " file is sent. ( Yesterday's File Complete ) ")
            else:
                dependencyLog.append(" As Well as " + str(
                    requiredSentFile.name) + " file is not yet sent.  (  Yesterday's File Complete ) ")

    return flag


def sendToMQ(FileName, FilePath, FileSize):
    # Creating producer
    URL = str(MessageProducerConfig["Host"])  # "localhost"
    PORT = MessageProducerConfig["Port"]  # 61613
    conn = stomp.Connection(host_and_ports=[(URL, PORT)], prefer_localhost=False)
    conn.connect(MessageProducerConfig["Username"], MessageProducerConfig["Password"], wait=True)

    # Creating Message
    MessageTemplate = MessageProducerConfig["Template"]
    MessageTemplate = MessageTemplate.replace("_FILE_NAME_", '"' + str(FileName) + '"')
    MessageTemplate = MessageTemplate.replace("_FILE_SIZE_", '"' + str(FileSize) + '"')
    MessageTemplate = MessageTemplate.replace("_File_Path_", '"' + str(FilePath) + '"')

    # Sending Messages
    conn.send(body=MessageTemplate, destination=MessageProducerConfig["Queue"])
    time.sleep(2)
    conn.disconnect()


def sendToKafka(FileName, FilePath, FileSize):
    # Creating Producer
    producer = KafkaProducer(
        bootstrap_servers=[str(MessageProducerConfig["Host"]) + ":" + str(MessageProducerConfig["Port"])],
        # sasl_plain_username=MessageProducerConfig["Username"],
        # sasl_plain_password=MessageProducerConfig["Password"],
        value_serializer=lambda x: dumps(x).encode('utf-8'),
        retries=5)
    # Creating Message
    MessageTemplate = MessageProducerConfig["Template"]
    for item in MessageTemplate:
        item['fileName'] = str(FileName)
        item['fileSize'] = FileSize
        item['directory'] = str(FilePath)
    # Sending Messages
    producer.send(MessageProducerConfig["TOPIC"], value=MessageTemplate)


def moveFile(downloaded_file, move_location, file):
    downloaded_file.rename(move_location / file)


def moveAndSendToUDL(downloaded_file, move_location, file):
    msgType = MessageProducerConfig["Type"]
    if msgType != 'PRINT':
        moveFile(downloaded_file, move_location, file)
        FileSize = os.stat(move_location / file).st_size
    if msgType == 'KAFKA':
        if LOG["logCommon"]:
            print("Sending a Message through KAFKA")
        sendToKafka(file, move_location, FileSize)
    elif msgType == "ACTIVEMQ":
        if LOG["logCommon"]:
            print("Sending a Message through ACTIVE MQ")
        sendToMQ(file, move_location, FileSize)
    elif msgType == "MOVE":
        if LOG["logCommon"]:
            print("Not Sending a Message to UDL")
    elif msgType == "PRINT":
        print("Not Sending a Message to UDL, Not Moving File")
    else:
        print("Please Give valid msgType")


def checkEligibility(location, file):
    reutersFile = location / file
    if LOG["logCommon"]:
        print(str(datetime.datetime.now()) + " Checking File " + str(reutersFile.name))
    global dependencyLog
    dependencyLog = []
    isEligible = True

    failedDependenciesCount = 0
    ignoredDependenciesCount = 0

    if LOG["logCommon"]:
        print(str(datetime.datetime.now()) + " Total " + str(len(
            dependencies[reutersFile.suffix])) + " Dependencies configured for " + reutersFile.suffix + " File")

    # Get both Validations for the suffix
    for dependency in dependencies[reutersFile.suffix]:
        isDependencyConfigured = True
        downloadedFileValidation = dependency['downloadedFileValidation']
        sentFileValidation = dependency['sentFileValidation']

        for validation in downloadedFileValidation:
            isDependencyConfigured = eval(validation)(reutersFile) and isDependencyConfigured

        if isDependencyConfigured:
            for condition in sentFileValidation:
                isEligible = eval(condition)(reutersFile) and isEligible

            if isEligible:
                printDependencySummery(LOG["logSuccessfulDependencies"], " Dependency Satisfied ")
            else:
                printDependencySummery(LOG["logFailedDependencies"], "  Dependency Failed  " + dependency['Comment'])
                failedDependenciesCount = failedDependenciesCount + 1
                break
        else:
            ignoredDependenciesCount = ignoredDependenciesCount + 1
            if sentFileValidation:
                dependencyLog.append(" No need to check ")
                for condition in sentFileValidation:
                    dependencyLog.append(" " + condition)
            printDependencySummery(LOG["logIgnoredDependencies"], " Dependency Ignored ")
            dependencyLog = []

    if isEligible:
        if ignoredDependenciesCount > 0:
            if LOG["logCommon"]:
                print(str(datetime.datetime.now()) + " " + str(ignoredDependenciesCount) + " Dependencies Ignored ")
        success_count = len(dependencies[reutersFile.suffix])
        success_count = success_count - ignoredDependenciesCount
        if success_count > 0:
            if LOG["logCommon"]:
                print(str(datetime.datetime.now()) + " " + str(success_count) + " Dependencies Satisfied ")
                print(str(
                    datetime.datetime.now()) + " Moving File " + reutersFile.name + " Sent Folder.")
        moveAndSendToUDL(reutersFile, sent_files_location, file)
    else:
        if LOG["logCommon"]:
            print(str(
                datetime.datetime.now()) + " Moving File " + reutersFile.name + " to Waiting Folder.")
        if MessageProducerConfig["Type"] != 'PRINT':
            moveFile(reutersFile, Waiting_files_location, file)
    if LOG["logCommon"]:
        print("    ---------------------------------------------    ")


def fileInterceptor():
    begin = time.time()
    # Check Newly downloaded files
    print(str(datetime.datetime.now()) + " Checking Eligibility of Newly downloaded files")
    print("    ---------------------------------------------    ")
    for fileName in download_files:
        try:
            if not filterFile(camel_download_location, fileName):
                checkEligibility(camel_download_location, fileName)
        except Exception as e:
            print(fileName)
            print(e)

    for i in range(0, config["Execution"]["RetryAttempt"]):
        # Check Waiting Files. Are they now eligible to send.
        if config["Execution"]["CheckWaiting"]:
            print(str(datetime.datetime.now()) + " Checking Eligibility of Waiting files")
            print("    ---------------------------------------------    ")
            for fileName in Waiting_files:
                checkEligibility(Waiting_files_location, fileName)
    end = time.time()
    print(f"Total runtime is {end - begin}")


#
# task = PythonOperator(
#     task_id='RefinitiveFileInterceptor',
#     python_callable=fileInterceptor,
#     op_kwargs={"x": "Apache Airflow"},
#     dag=dag)
#
# task()

fileInterceptor()
