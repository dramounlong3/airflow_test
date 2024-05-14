import os
import glob
import time
import inspect
import shutil
from loguru import logger
from urllib import request as ur_req
import pymssql
import pandas as pd
from datetime import datetime, timedelta


def database_conn():
    # other
    # return a db connection

    # conn_args = get_conn_info("GLOBAL_MSSQLDB_DATA_ALERT")
    try:
        # conn = pymssql.connect(**conn_args)
        file_name = os.path.basename(__file__)
        function_name = inspect.currentframe().f_code.co_name
        logger.info(f"file nmae: {file_name} ,function name: {function_name}")
        conn = pymssql.connect(host = "localhost", user = "sa", password = "19890729", database = "testdb")
    except Exception as e:
        logger.error("This is an error message.")
    cursor = conn.cursor()
    return (conn, cursor)


# def scan_file(ti):
def scan_file():
    # \\deltafileserver\tableaureport\Data Alert\<Project>\Setting
    # 0.檢查專案資料夾內是否有excel
    # 0-1.project_name, project_path, is_exist in a list
    # scan_folder = r'\\deltafileserver\tableaureport\Data Alert'
    scan_folder = r'D:\deltafileserver\tableaureport\Data Alert'
    
    conn, cur = None, None

    try:
        conn, cur = database_conn()

        # sql_query = '''
        #     SELECT * FROM BI_Data_Alert.dbo.Project
        # '''
        
        sql_query = '''
            SELECT * FROM testdb.dbo.Project
        '''

        cur.execute(sql_query)
        columns = [column[0] for column in cur.description]
        data = cur.fetchall()

        df = pd.DataFrame(data, columns = columns)
        
        project_folder_list = []
        for project_name in df['Project_Name']:
            project_path = os.path.join(scan_folder, project_name, 'Setting', 'Alert & Setting.xlsx')
            project_setting_file_is_exists = os.path.exists(project_path)
            project_folder = {
                "project_name": project_name,
                "project_path": project_path,
                "project_setting_file_is_exists": project_setting_file_is_exists
            }
            project_folder_list.append(project_folder)
            
    except Exception as e:
        logger.error("This is an error message.")
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()

    print("project_folder_list", project_folder_list)
    if any(item['project_setting_file_is_exists'] for item in project_folder_list):
        # ti.xcom_push(key = 'project_folder_list', value = project_folder_list)
        # return 'download_file'
        return project_folder_list
    else:
        return 'end_task'
    
    # pass

class File_management:
    # def download_file(self, ti):
    def download_file(self, project_folder_list):
        # 1.根據scan_file的結果, 有檔案就download
        # 1-1.download file
        # 1-2. sys_log.download_success / sys_log.download_fail
        # 1-3. db.download_success / db.download_fail
        # project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        for idx, project_folder in enumerate(pf for pf in project_folder_list if pf['project_setting_file_is_exists']):
            source_path = project_folder['project_path']
            target_path = source_path.replace("D:\\deltafileserver\\tableaureport","")
            project_folder_list[idx]['target_path'] = target_path
            
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
        # 2-2. sys_log.read_success / sys_log.read_fail
        # 2-3. db.read_success / db.read_fail
        # project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        
        df_setting_file_list = []
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                try:
                    df_setting_file = pd.read_excel(project_folder['target_path'])
                    df_setting_file_list.append(df_setting_file)
                except Exception as e:
                    logger.error(f"Error loading Excel data: {str(e)}")

    def backup_file():
        # 5.備份excel
        # 5-1. copy excel to Backup folder
        # 5-2. sys_log.backup_success / sys_log.backup_fail
        # 5-3. db.backup_success / db.backup_fail
        print("File_management.backup_file")
        pass

    def remove_file():
        # 6.刪除original excel
        # 6-1. remove file
        # 6-2. sys_log.remove_success / sys_log.remove_fail
        # 6-3. db.remove_success / db.remove_fail
        print("File_management.remove_file")
        pass
    
class Notification:
    def send_mail():
        # 4.根據3-1 ~ 3-3的結果決定是否通知
        # 4-1. if field failed => notify_[owner]_user
        # 4-2. sys_log.notify_owner_user_success / sys_log.notify_owner_user_fail
        # 4-3. db.notify_owner_user_success / db.notify_owner_user_fail

        # 4-4. if rule expression FALSE/TRUE => notify_[mail_to]_user #條件是否達標都通知
        # 4-2. sys_log.notify_[mail_to]_user_success / sys_log.notify_[mail_to]_user_fail
        # 4-3. db.notify_[mail_to]_user_success / db.notify_[mail_to]_user_fail
        print("Notification.send_mail")
        pass

class Validation:
    def __init__(self) -> None:
        self.notify = Notification()
    
    def validation_field():
        # 3.驗證欄位正確性及rule是否達標
        # 3-1. verify rls field
        # 3-2. sys_log.verify_field_success / sys_log.verify_field_fail
        # 3-3. db.verify_field_success / db.verify_field_fail
        print("Validation.validation_field")
        pass

    def validation_rule():
        # 3-4. verify rule expression
        # 3-2. alert_log.verify_rule_success / alert_log.verify_rule_fail
        # 3-3. db.verify_rule_success / db.verify_rule_fail
        print("Validation.validation_rule")
        pass



    # scan_file >> [download_file, end_task]
    # download_file >> read_file >> validation_field >> [validation_rule, end_task]
    # validation_rule >> backup_file >> remove_file >> end_task
    
def main_flow():
    scan_file_result = scan_file()
    print("type(scan_file_result)", type(scan_file_result))
    fm = File_management()
    download_file_result = fm.download_file(scan_file_result)
    fm.read_file(download_file_result)
    

main_flow()