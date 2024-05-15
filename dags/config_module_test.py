import os
import glob
import time
import inspect
import shutil
import re
from loguru import logger
from urllib import request as ur_req
import pymssql
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def database_conn():
    # other
    # return a db connection

    # conn_args = get_conn_info("GLOBAL_MSSQLDB_DATA_ALERT")
    try:
        # conn = pymssql.connect(**conn_args)
        file_name = os.path.basename(__file__)
        function_name = inspect.currentframe().f_code.co_name
        logger.info(f"file name: {file_name} ,function name: {function_name}")
        conn = pymssql.connect(host = "localhost", user = "sa", password = "19890729", database = "testdb")         #home
        # conn = pymssql.connect(host = "TWTPESQLDV2", database = "BI_Data_Alert")                                     #company
    except Exception as e:
        logger.error(f"{function_name} DB connection error. {str(e)}")
    cursor = conn.cursor()
    return (conn, cursor)


# def scan_file(ti):
def scan_file():
    # \\deltafileserver\tableaureport\Data Alert\<Project>\Setting
    # 0.檢查專案資料夾內是否有excel
    # 0-1.project_name, project_path, is_exist in a list
    scan_folder = r'D:\deltafileserver\tableaureport\Data Alert'  #home
    # scan_folder = r'\\deltafileserver\tableaureport\Data Alert'     #company
    
    conn, cur = None, None
    project_folder_list = []

    try:
        conn, cur = database_conn()

        sql_query = '''
            SELECT * FROM BI_Data_Alert.dbo.Project
        '''

        cur.execute(sql_query)
        columns = [column[0] for column in cur.description]
        data = cur.fetchall()

        df = pd.DataFrame(data, columns = columns)
        
        setting_folder = 'Setting'
        original_file_name = 'Alert & Rule Setting.xlsx'
        
        # DB project 和 cloud storage都存在的才會被檢查
        for project_name in df['Project_Name']:
            project_path = os.path.join(scan_folder, project_name, setting_folder, original_file_name)
            project_setting_file_is_exists = os.path.exists(project_path)
            project_folder = {
                "project_name": project_name,
                "project_path": project_path,
                "project_setting_file_is_exists": project_setting_file_is_exists
            }
            project_folder_list.append(project_folder)
            
    except Exception as e:
        logger.error(f"DB execute error. {str(e)}")
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()

    if any(item['project_setting_file_is_exists'] for item in project_folder_list):
        # ti.xcom_push(key = 'project_folder_list', value = project_folder_list)
        # return 'download_file'
        return project_folder_list
    else:
        return 'end_task'
        # return project_folder_list
    
    # pass

class File_management:
    # def download_file(self, ti):
    def download_file(self, project_folder_list):
        # 1.根據scan_file的結果, 有檔案就download
        # 1-1.download file
        # 1-2. sys_log.download_success / sys_log.download_fail
        # 1-3. db.download_success / db.download_fail

        # project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        # for idx, project_folder in enumerate(pf for pf in project_folder_list if pf['project_setting_file_is_exists']):
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                source_path = project_folder['project_path']
                target_path = source_path.replace(r"D:\deltafileserver\tableaureport","")    #home
                # target_path = source_path.replace(r"\\deltafileserver\tableaureport","")       #company
                project_folder['target_path'] = target_path

                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                try:
                    shutil.copy2(source_path, target_path)
                    # ti.xcom_push(key = 'project_folder_list', value = project_folder_list)
                except Exception as e:
                    logger.error(f"error: {str(e)}")
        return project_folder_list
    
    # def read_file(self, ti):
    def read_file(self, project_folder_list):
        # 2.讀取excel
        # 2-1. read file
        # 2-2. remove local file?
        # 2-3. sys_log.read_success / sys_log.read_fail
        # 2-4. db.read_success / db.read_fail
        # project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')

        df_setting_content_list = []
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                try:
                    df_setting_content = pd.read_excel(project_folder['target_path'], sheet_name=None) #讀取所有sheet
                    df_setting_content_list.append(df_setting_content)
                except Exception as e:
                    logger.error(f"Error loading Excel data: {str(e)}")
        # ti.xcom_push(key = 'df_setting_content_list', value = df_setting_content_list) # or save content to DB?
        return df_setting_content_list

    # def backup_file(self, ti):
    def backup_file(self, project_folder_list):
        # 5.備份excel
        # 5-1. copy excel to Backup folder
        # 5-2. sys_log.backup_success / sys_log.backup_fail
        # 5-3. db.backup_success / db.backup_fail
        # project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                source_path = os.path.dirname(project_folder['project_path'])
                backup_filename, backup_extension = os.path.splitext(os.path.basename(project_folder['project_path']))
                current_date = datetime.now().strftime('%Y-%m-%d')

                # 比對Backup資料夾內的檔名規則
                # Alert & Setting yyyy-mm-dd_xx.xlsx
                pattern = re.compile(r'{0} {1}_(\d+){2}'.format(backup_filename ,re.escape(current_date), backup_extension))

                target_path = os.path.join(source_path, "Backup")
                # 建立Backup
                os.makedirs(target_path, exist_ok=True)

                # 確認當日最大的序號
                serial_nums = [
                    int(pattern.match(filename).group(1))
                    for filename in os.listdir(target_path)
                    if pattern.match(filename)
                ] if os.path.exists(target_path) else []
                print("serial_nums", serial_nums)

                next_serial_num = max(serial_nums, default=0) + 1
                next_serial_num = f'{next_serial_num:02}'

                # 儲存檔名
                save_filename = '{0} {1}_{2}{3}'.format(backup_filename, current_date, next_serial_num, backup_extension)

                target_path_file = os.path.join(source_path, "Backup", save_filename)
                source_path_file = project_folder['project_path']

                try:
                    shutil.copy2(source_path_file, target_path_file)
                except Exception as e:
                    logger.error(f"error: {str(e)}")

    # def remove_file(self, ti):
    def remove_file(self, project_folder_list):
        # 6.刪除original excel
        # 6-1. remove file
        # 6-2. sys_log.remove_success / sys_log.remove_fail
        # 6-3. db.remove_success / db.remove_fail
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                remove_path = project_folder['project_path']

                try:
                    os.remove(remove_path)
                    logger.success(f'{remove_path} Successfully remove file')
                except Exception as e:
                    logger.error(f"{remove_path} Failed to remove file. {str(e)}")


class Notification:
    
    def __init__(self):
        self.smtp_server = 'deltarelay.deltaww.com'
        self.smtp_port = '25'
        self.email_sender = 'IT.BIServices@deltaww.com'  #kyle.guo@deltaww.com

    
    # def send_mail(self, ti):
    def send_mail(self, email_subject, email_body, Alert_ID, Rule_ID):
        # 4.根據3-1 ~ 3-3的結果決定是否通知
        # 4-1. if field failed => notify_[owner]_user
        # 4-2. sys_log.notify_owner_user_success / sys_log.notify_owner_user_fail
        # 4-3. db.notify_owner_user_success / db.notify_owner_user_fail

        # 4-4. if rule expression FALSE/TRUE => notify_[mail_to]_user #條件是否達標都通知
        # 4-2. sys_log.notify_[mail_to]_user_success / sys_log.notify_[mail_to]_user_fail
        # 4-3. db.notify_[mail_to]_user_success / db.notify_[mail_to]_user_fail

        # sender_password = ''
        
        email_receiver = None
        conn, cur = None, None
        try:
            conn, cur = database_conn()

            sql_query = f'''
                SELECT top 1 Mail_To
                FROM BI_Data_Alert.dbo.Alert
                WHERE Alert_ID = '{Alert_ID}'
                AND Rule_ID = '{Rule_ID}'
            '''
            
            cur.execute(sql_query)
            data = cur.fetchone()

            if data is None:
                raise ValueError
            
            email_receiver = data[0]
            
        except ValueError as e:
            logger.warning(f"No records found. {str(e)}")
            return "DB has no records"
        except Exception as e:
            logger.error(f"DB error {str(e)}")
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
        
        email = MIMEMultipart()
        email['From'] = self.email_sender
        email['To'] = email_receiver
        email['Subject'] = email_subject
        email.attach(MIMEText(email_body, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                # server.login(sender_email, sender_password)
                server.send_message(email)
                logger.success("Successfully send e-mail")
        except Exception as e:
            logger.error(f'Failed to send e-mail. {str(e)}')


class Validation:
    def __init__(self) -> None:
        self.notify = Notification()
    
    def validation_rls_field(self, df_setting_content_list):
        # 3.驗證欄位正確性及rule是否達標
        # 3-1. verify rls field
        # 3-2. sys_log.verify_field_success / sys_log.verify_field_fail
        # 3-3. db.verify_field_success / db.verify_field_fail
        # print("df_setting_content_list", df_setting_content_list)
        
        rule_field_names = None
        conn, cur = None, None
        try:
            conn, cur = database_conn()

            sql_query = f'''
                SELECT Rule_Field_Name
                FROM BI_Data_Alert.dbo.Rule_Field
            '''
            
            cur.execute(sql_query)
            data = cur.fetchall()

            if data is None:
                raise ValueError
            
            rule_field_names = [row[0] for row in data]
            rule_field_names.append('Rule_ID')
            rule_field_names.append('Owner_Emp_Code')
            rule_field_names.append('Owner_Mail')
            rule_field_names.append('Rule Expression')
            
            
        except ValueError as e:
            logger.warning(f"No records found. {str(e)}")
            return "DB has no records"
        except Exception as e:
            logger.error(f"DB error {str(e)}")
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
        
        print("\nrule_field_names\n", rule_field_names)
        
        rls_column = [column.strip() for column in list(df_setting_content_list[0].get('Rule_Setting').columns)]
        print("\nrls_column\n", rls_column)
        
        # 檢查DB、excel所有欄位是否都存在
        missing_columns = None
        rls_column_verify = None
        if set(rls_column).issubset(rule_field_names):
            print("All columns in rls_column exist in rule_field_names.")
            rls_column_verify = True
        else:
            missing_columns = set(rls_column) - set(rule_field_names)
            rls_column_verify = False

        print("\nmissing_columns\n:", missing_columns, "\nrls_column_verify", rls_column_verify)
        

    def validation_rule_field():
        # 3-4. verify rule field
        # 3-2. alert_log.verify_rule_success / alert_log.verify_rule_fail
        # 3-3. db.verify_rule_success / db.verify_rule_fail
        print("Validation.validation_rule")
        pass



    # scan_file >> [download_file, end_task]
    # download_file >> read_file >> validation_field >> [validation_rule, end_task]
    # validation_rule >> backup_file >> remove_file >> end_task
    
def main_flow():
    scan_file_result = scan_file()
    fm = File_management()
    notify = Notification()
    verify = Validation()

    download_file_result = fm.download_file(scan_file_result)
    read_file_result = fm.read_file(download_file_result)
    verify.validation_rls_field(read_file_result)
    notify.send_mail('test_subject', 'test_body', 'IGSM-2_ALERT-2', 'IGSM-3')
    # fm.backup_file(download_file_result)
    # fm.remove_file(download_file_result)
    

main_flow()