import paramiko
import datetime
import sys

now = datetime.datetime.now()


def downloadSFTPFile(sftpHost, sftpPort, sftpUser, sftpPassword, remoteDir, remoteFileName, localFileName, PrintFiles):
    # Connect to the SFTP server
    transport = paramiko.Transport((sftpHost, sftpPort))
    transport.connect(username=sftpUser, password=sftpPassword)
    # Create an SFTP client object
    sftp = paramiko.SFTPClient.from_transport(transport)
    # Change to the remote directory
    sftp.chdir(remoteDir)
    # List the files in the remote directory and print
    fileList = sftp.listdir()
    if PrintFiles: print("Listing all Files inside directory until Found ")
    for fileName in fileList:
        if PrintFiles: print(fileName)
        if fileName == remoteFileName:
            print(f'Downloading {fileName}')
            sftp.get(remoteFileName, localFileName)
            break
    # Close the SFTP session
    sftp.close()
    transport.close()
    pass


def download_DGCX_456():
    print("Configuration for DGCX DAQ(456)")
    # 1.SFTP HOST
    sftp_host = 'eosftp.dgcx.ae'
    print(f'1. SFTP HOST : {sftp_host} ')

    # 2.SFTP PORT
    sftp_port = 6022
    print(f'2. SFTP PORT : {sftp_port} ')

    # 3.SFTP USER
    sftp_user = 'eosftp'
    print(f'3. SFTP USER : {sftp_user}')

    # 4.SFTP PASSWORD
    sftp_password = 'eos1234'
    print(f'4. SFTP PASSWORD :{sftp_host} ')

    # 5.Remote Directory
    file_Date = now - datetime.timedelta(days=4)
    file_Name_Date = file_Date.strftime('%Y%m%d')
    dirDate_yy_mm = now.strftime('%Y-%m')
    remote_dir = f'/Common/ContractMaster/{dirDate_yy_mm}'
    print(f'5. Remote Directory : {remote_dir} ')

    # 6.Remote File Name
    remote_file_name = f'ContractMaster_{file_Name_Date}.csv'
    print(f'6. Remote File Name : {remote_file_name}')

    # 7. Local File Name NOTE : Create The directory manually
    local_file_name = str(f'/daq01/dgcx/' + remote_file_name)
    print(f'7. Local File Name : {local_file_name}')

    # 7. List all the files
    print_Files = True
    print(f'8. List and Print Files :  {print_Files}')

    print("Downloading File")
    downloadSFTPFile(sftp_host, sftp_port, sftp_user, sftp_password, remote_dir, remote_file_name, local_file_name,
                     print_Files)
    pass


def download_Test_UAT():
    print("Configuration for DGCX DAQ(456)")
    # 1.SFTP HOST
    sftp_host = '192.168.118.132'
    print(f'1. SFTP HOST : {sftp_host} ')

    # 2.SFTP PORT
    sftp_port = 22
    print(f'2. SFTP PORT : {sftp_port} ')

    # 3.SFTP USER
    sftp_user = 'udm'
    print(f'3. SFTP USER : {sftp_user}')

    # 4.SFTP PASSWORD
    sftp_password = 'rdu@OPS@UAT2017!'
    print(f'4. SFTP PASSWORD :{sftp_host} ')

    # 5.Remote Directory

    remote_dir = '/home/udm/'
    print(f'5. Remote Directory : {remote_dir} ')

    # 6.Remote File Name
    remote_file_name = 'hello.txt'
    print(f'6. Remote File Name : {remote_file_name}')

    # 7. Local File Name
    local_file_name = 'Downloaded_123.txt'
    print(f'7. Local File Name : {remote_file_name}')

    # 7. Local File Name
    print_Files = True
    print(f'8. List and Print Files :  {print_Files}')

    print("Downloading File")
    downloadSFTPFile(sftp_host, sftp_port, sftp_user, sftp_password, remote_dir, remote_file_name, local_file_name,
                     print_Files)
    pass


# run time parameter
print(f'Executing {str(sys.argv[1])}')
eval(str(sys.argv[1]))
