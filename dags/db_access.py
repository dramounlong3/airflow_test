# from common.db_tools import get_conn_info
from event_log import Event_log
import pymssql

def database_conn():
    # other
    # return a db connection
    
    # file_name = os.path.basename(__file__)
    # function_name = inspect.currentframe().f_code.co_name
    # logger.info(f"file name: {file_name} ,function name: {function_name}")

    # conn_args = get_conn_info("GLOBAL_MSSQLDB_DATA_ALERT")
    try:
        Event_log.log_message('trace', 'Start to connect DB.')
        
        conn = pymssql.connect(host = "172.21.176.1", user = "sa", password = "19890729", database = "BI_Data_Alert")         #home
        # conn = pymssql.connect(**conn_args)                                                                                 #company
        
        Event_log.log_message('success', 'Successfully conncet to DB.')
    except Exception as e:
        Event_log.log_message('error', f'{str(e)}')
    cursor = conn.cursor()
    return (conn, cursor)