from event_log import Event_log
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
import re

class File_management:
    def download_file(self, ti):
        # 1.根據scan_file的結果, 有檔案就download
        # 1-1.download file
        # 1-2. sys_log.download_success / sys_log.download_fail
        # 1-3. db.download_success / db.download_fail

        Event_log.log_message('trace', 'Start to download file.')

        project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                source_path = project_folder['project_path']
                # target_path = source_path.replace(r"D:\deltafileserver\tableaureport","")    #home
                # target_path = source_path.replace(r"\\deltafileserver\tableaureport","")       #company
                target_path = source_path.replace(r"/deltafileserver/tableaureport","")       #home linux
                project_folder['target_path'] = target_path

                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                
                try:
                    shutil.copy2(source_path, target_path)
                    Event_log.log_message('success', f'Successfully download file from source path: [{source_path}] to target path: [{target_path}]')
                except Exception as e:
                    Event_log.log_message(f"error: {str(e)}")
            else:
                project_folder['target_path'] = None
        ti.xcom_push(key = 'project_folder_list', value = project_folder_list)
    
    def read_file(self, ti):
        # 2.讀取excel
        # 2-1. read file
        # 2-2. remove local file and save to DB?
        # 2-3. sys_log.read_success / sys_log.read_fail
        # 2-4. db.read_success / db.read_fail
        
        Event_log.log_message('trace', 'Start to read file.')
        project_folder_list = ti.xcom_pull(task_ids = 'download_file', key = 'project_folder_list')

        df_setting_content_list = []
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                try:
                    df_setting_content = pd.read_excel(project_folder['target_path'], sheet_name=['Alert_Setting','Rule_Setting'])
                    df_setting_content_list.append(df_setting_content)
                    Event_log.log_message('success', f"Successfully read file from path: [{project_folder['target_path']}]")
                except Exception as e:
                    Event_log.log_message('error', f'{str(e)}')
        ti.xcom_push(key = 'df_setting_content_list', value = df_setting_content_list) 

    def backup_file(self, ti):
        # 5.備份excel
        # 5-1. copy excel to Backup folder
        # 5-2. sys_log.backup_success / sys_log.backup_fail
        # 5-3. db.backup_success / db.backup_fail

        Event_log.log_message('trace', 'Start to backup file.')

        project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                try:
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

                    next_serial_num = max(serial_nums, default=0) + 1
                    next_serial_num = f'{next_serial_num:02}'

                    # 儲存檔名
                    save_filename = '{0} {1}_{2}{3}'.format(backup_filename, current_date, next_serial_num, backup_extension)

                    target_path_file = os.path.join(source_path, "Backup", save_filename)
                    source_path_file = project_folder['project_path']

                    shutil.copy2(source_path_file, target_path_file)
                    Event_log.log_message('success', f'Successfully backup file from [{source_path_file}] to [{target_path_file}]')
                except Exception as e:
                    Event_log.log_message('error', f'{str(e)}')

    def remove_file(self, ti):
        # 6.刪除original excel
        # 6-1. remove file
        # 6-2. sys_log.remove_success / sys_log.remove_fail
        # 6-3. db.remove_success / db.remove_fail

        Event_log.log_message('trace', 'Start to remove file.')
        
        project_folder_list = ti.xcom_pull(task_ids = 'scan_file', key = 'project_folder_list')
        for project_folder in project_folder_list:
            if project_folder['project_setting_file_is_exists']:
                remove_path = project_folder['project_path']
                try:
                    os.remove(remove_path)
                    Event_log.log_message('success', f'Successfully remove current file from [{remove_path}].')
                except Exception as e:
                    Event_log.log_message('error', f'{str(e)}')

            # housekeeping backup file
            Event_log.log_message('trace', 'Start to remove backup file.')
            backup_path = os.path.join(os.path.dirname(project_folder['project_path']), 'Backup')
            is_remove_backup = False
            one_month_age = datetime.now() - timedelta(days=30)

            for filename in os.listdir(backup_path):
                try:
                    remove_backup_path = os.path.join(backup_path, filename)
                    if os.path.isfile(remove_backup_path) and os.path.getctime(remove_backup_path) < one_month_age.timestamp():
                        os.remove(remove_backup_path)
                        is_remove_backup = True
                        Event_log.log_message('success', f'Successfully remove backup file from [{remove_backup_path}].')
                except Exception as e:
                    Event_log.log_message('error', f'{str(e)}')

            if not is_remove_backup:
                Event_log.log_message('info', f'There are no backup file must be remove from [{backup_path}].')
