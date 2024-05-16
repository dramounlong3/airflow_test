from event_log import Event_log
from db_access import database_conn
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import smtplib

class Notification:
    def __init__(self):
        self.smtp_server = 'deltarelay.deltaww.com'
        self.smtp_port = '25'
        self.email_sender = 'IT.BISERVICES@deltaww.com'  #kyle.guo@deltaww.com

    def send_mail(self, email_subject, email_body, Alert_ID, Rule_ID):
        # 4.根據3-1 ~ 3-3的結果決定是否通知
        # 4-1. if field failed => notify_[owner]_user
        # 4-2. sys_log.notify_owner_user_success / sys_log.notify_owner_user_fail
        # 4-3. db.notify_owner_user_success / db.notify_owner_user_fail

        # 4-4. if rule expression FALSE/TRUE => notify_[mail_to]_user #條件是否達標都通知
        # 4-2. sys_log.notify_[mail_to]_user_success / sys_log.notify_[mail_to]_user_fail
        # 4-3. db.notify_[mail_to]_user_success / db.notify_[mail_to]_user_fail

        Event_log.log_message('trace', f'Start to query dbo.Alert where Alert_ID = {Alert_ID} and Rule_ID = {Rule_ID}')

        # email_password = ''
        
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

            Event_log.log_message('success', f'Successfully query dbo.Alert where Alert_ID = {Alert_ID} and Rule_ID = {Rule_ID}')
        except ValueError as e:
            Event_log.log_message('warning', f"No records found.")
            return "No records found."
        except Exception as e:
            Event_log.log_message('error', f'{str(e)}')
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
        
        Event_log.log_message('trace', 'Strart to send email.')

        email = MIMEMultipart()
        email['From'] = self.email_sender
        email['To'] = email_receiver
        email['Subject'] = email_subject
        email.attach(MIMEText(email_body, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                # server.login(email_sender, email_password)
                server.send_message(email)
                Event_log.log_message('success', f'Successfully send e-mail to {email_receiver}.')
        except Exception as e:
            Event_log.log_message('error', f'{str(e)}')