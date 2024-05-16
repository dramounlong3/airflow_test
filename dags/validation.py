from event_log import Event_log
from db_access import database_conn
from notification import Notification

import re


class Validation:
    def __init__(self):
        self.notify = Notification()

    def validation_rls_field(self, ti):
        # 3.驗證欄位正確性及rule是否達標
        # 3-1. verify rls field
        # 3-2. sys_log.verify_field_success / sys_log.verify_field_fail
        # 3-3. db.verify_field_success / db.verify_field_fail
        Event_log.log_message('trace', 'Start to validate rls field.')
        
        df_setting_content_list = ti.xcom_pull(task_ids='read_file', key='df_setting_content_list')
        
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
            
            rule_field_names = [row[0].strip() for row in data]
            rule_field_names.append('Rule_ID')
            rule_field_names.append('Owner_Emp_Code')
            rule_field_names.append('Owner_Mail')
            rule_field_names.append('Rule_Expression')
            
            Event_log.log_message('success', f'Successfully query dbo.Rule_Field')
        except ValueError as e:
            Event_log.log_message('warning', f"No records found.")
            return "No records found."
        except Exception as e:
            Event_log.log_message(f"{str(e)}")
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
        
        Event_log.log_message('trace', 'Start to get rls_column from Rule Setting')
        # df_setting_content_list為多張excel的dataframe, 須根據當前要比較的project選擇對應的content, 再讀取sheet
        rls_column = [column.strip() for column in list(df_setting_content_list[0].get('Rule_Setting').columns)]
        Event_log.log_message('trace', 'dead')
        # 檢查excel所有欄位是否都存在於DB定義中
        undefine_columns = None
        rls_column_verify = None
        if set(rls_column).issubset(rule_field_names):
            rls_column_verify = True
            Event_log.log_message('success', 'All rls_columns are exist in DB.')
        else:
            undefine_columns = set(rls_column) - set(rule_field_names)
            rls_column_verify = False
            Event_log.log_message('error', f'RLS columns {undefine_columns} are NOT exist in DB.')
            # call notify.send_mail

        verify_result_dict = {
            "rls_column_verify": rls_column_verify,
            "undefine_columns": undefine_columns
        }
        return verify_result_dict
        
    def validation_rule_field(self, ti):
        # 3-4. verify rule field
        # 3-2. alert_log.verify_rule_success / alert_log.verify_rule_fail
        # 3-3. db.verify_rule_success / db.verify_rule_fail
        Event_log.log_message('trace', 'Start to validate rule field.')
        
        df_setting_content_list = ti.xcom_pull(task_ids='read_file', key='df_setting_content_list')

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
            
            rule_field_names = [row[0].strip() for row in data]
            rule_field_names.append('Rule_ID')
            rule_field_names.append('Owner_Emp_Code')
            rule_field_names.append('Owner_Mail')
            rule_field_names.append('Rule Expression')
            
            Event_log.log_message('success', f'Successfully query dbo.Rule_Field')
        except ValueError as e:
            Event_log.log_message('warning', f"No records found.")
            return "No records found."
        except Exception as e:
            Event_log.log_message('error', f'{str(e)}')
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
        
        Event_log.log_message('trace', 'Start to get rule fields from Rule Setting')
        # df_setting_content_list為多張excel的dataframe, 須根據當前要比較的project選擇對應的content, 再讀取sheet
        rule_column = list(df_setting_content_list[0].get('Rule_Setting')['Rule_Expression'])

        rule_content_pattern = r'\[(.*?)\]'
        rule_content_list = []

        for item in rule_column:
            rule_content_list.extend(re.findall(rule_content_pattern, item))
        
        # 檢查excel所有欄位是否都存在於DB定義中
        undefine_columns = None
        rule_content_list_verify = None
        if set(rule_content_list).issubset(rule_field_names):
            rule_content_list_verify = True
            Event_log.log_message('success', 'All rule fields are exist in DB.')
        else:
            undefine_columns = set(rule_content_list) - set(rule_field_names)
            rule_content_list_verify = False
            Event_log.log_message('error', f'Rule field {undefine_columns} are NOT exist in DB.')
            # call notify.send_mail

        verify_result_dict = {
            "rule_column_verify": rule_content_list_verify,
            "undefine_columns": undefine_columns
        }

        return verify_result_dict