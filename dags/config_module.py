# import os
# import time
# import logging
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator

# def check_weekday(date_stamp):
#     print("check_weekday")
#     today = datetime.strptime(date_stamp, '%Y%m%d')
#     if today.isoweekday() <= 5:
#         return 'is_working_day'
#     else:
#         return 'is_holiday'
    
# def get_metadata():
#     print('get_metadata' + '~' *30 + + '!!!')
#     logging.info('get_metadata' + '~' *30 + + '!!!')

# def clean_data():
#     print('clean_data' + '~' * 30 + '!!!')
#     logging.info('clean_data' + '~' * 30 + '!!!')

# default_args = {
#     'owner': 'Kyle',
#     'start_date': datetime(2024, 5, 10),
#     'schedule_interval': '@daily',
#     'tags': ['Test'],
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5)
# }

# with DAG(dag_id='toturial', default_args = default_args) as dag:
#     tw_stock_start = DummyOperator(
#         task_id = 'tw_stock_start'
#     )

#     check_weekday = BranchPythonOperator(
#         task_id = 'check_weekday',
#         python_callable = check_weekday,
#         op_args=['{{ ds_nodash }}'] #ds_nodash是執行日的日期, 20240511, 作為呼叫check_weekday時代過去的參數 => check_weekday(ds_nodash)
#     )

#     is_workday = DummyOperator(
#         task_id = 'is_workday'
#     )

#     is_holiday = DummyOperator(
#         task_id = 'is_holiday'
#     )

#     get_metadata = PythonOperator(
#         task_id  = 'get_metadata',
#         python_callable = get_metadata
#     )

#     clean_data = PythonOperator(
#         task_id = 'clean_data',
#         python_callable = clean_data
#     )

#     tw_stock_end = DummyOperator(
#         task_id = 'tw_stock_end',
#         trigger_rule = 'one_success'
#     )

#     tw_stock_start >> check_weekday >> [is_workday, is_holiday]
#     is_holiday >> tw_stock_end
#     is_workday >> get_metadata >> clean_data >> tw_stock_end

# =================================================================================================

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# bash operator

with DAG(
    dag_id ='simple_dag',
    start_date = datetime(2024, 5, 11),
    schedule_interval = '@daily'
) as dag: 
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = "echo start!!"
    )
    
    task2 = BashOperator(
        task_id = 'task2',
        bash_command = "echo finish!!"
    )
    
    task1 >> task2

from airflow.operators.python import PythonOperator

# python operator

def say_hello():
    print("hello")
    
with DAG(
    dag_id = 'python_dag',
    start_date = datetime(2024,5,11),
    schedule_interval = None
) as dag:
    task3 = PythonOperator(
        task_id='task3',
        python_callable = say_hello
    )
    
    task3
    
# xcoms (在function間互相溝通的暫存區, 只適合用在少量資料)
# 先呼叫send_name push xcom的內容後, 再呼叫hello_name pull xcom的內容, 並給定task_ids和key值
# ti是固定的變數名稱, task instance, 取別的會出錯
# task_ids指的是operator的task_id
def hello_name(task_instance):
    name = task_instance.xcom_pull(task_ids = 'task99', key='lastname')
    result = task_instance.xcom_pull(task_ids = 'task99', key='return_value')
    print('Hello', name)
    print('my return_value', result)
    
def send_name(task_instance):
    task_instance.xcom_push(key = 'lastname', value = 'Shawn')
    return "abcd"
    
    
with DAG(
    dag_id = 'xcoms_dag',
    start_date = datetime(2024,5,11),
    schedule_interval = None
) as dag:
    task4 = PythonOperator(
        task_id = 'task4',
        python_callable = hello_name
    )
    
    task5 = PythonOperator(
        task_id = 'task99',
        python_callable = send_name
    )
    
    task5 >> task4


# Variable
# 三種設定方式: 
#   1.從UI admin => variable
#   2.從terminal設定 (設定後在UI也可以看到)
#       *一般變數 airflow variables set myKey myValue 
#       *json變數 airflow variables set -j myJKey '{"my_real_key": "my_real_value"}'
#   3.透過程式設定
# 兩種取得方式
#   1.Variable.get
#   2.**context
from airflow.models import Variable
import json

def set_var():
    # 這裡只有code var, 其他地方設定的要直接從UI看
    # serialize和deserialize無效果, 故改為json.dumps和json.loads
    Variable.set(key='var_from_code', value='var_value_from_code')
    Variable.set(key='var_from_code_j', value=json.dumps({'var_key_from_code': 'var_value_form_code_json'}), serialize_json=True) #實測serialize無效果, 序列化後仍是字串
    print("set_var")

def get_var():
    ui_var = Variable.get('var_from_ui')
    terminal_var = Variable.get('var_from_terminal')
    terminal_json_var = json.loads(Variable.get("var_from_terminal_json", deserialize_json=True)) #實測deserialize無效果, 解析後仍是字串
    code_var = Variable.get('var_from_code')
    code_json_var = json.loads(Variable.get('var_from_code_j', deserialize_json=True))  #實測deserialize無效果, 解析後仍是字串
    my_json_var = json.loads(Variable.get("my_key2", deserialize_json=True))            #實測deserialize無效果, 解析後仍是字串
    
    print("Variable.get('var_from_ui'):", ui_var)
    print("Variable.get('var_from_terminal'):", terminal_var)
    print("Variable.get('var_from_terminal_json', deserialize=True): " + terminal_json_var['my_real_key'])
    print("Variable.get('var_from_code'):", code_var)
    print("Variable.get('var_from_code_j', deserialize=True):" + code_json_var['var_key_from_code'])
    print("my_json_var['key_in_json']", my_json_var['key_in_json'])

# **context是airflow內建變數
def get_var_by_context(**context):
    # 仍然無法透過json關鍵字取得json格式的value, 都要先用json.loads轉換
    # 改用context取得get_var()的所有變數
    c_ui_var = context['var']['value'].get('var_from_ui')
    c_terminal_var = context['var']['value'].get('var_from_terminal')
    c_terminal_json_var =  json.loads(context['var']['json'].get('var_from_terminal_json'))['my_real_key']
    c_code_var = context['var']['value'].get('var_from_code')
    c_code_json_var = json.loads(context['var']['json'].get('var_from_code_j'))['var_key_from_code'] #用['var']['value'] 沒辦法取得json格式的變數, 但取出來還是得用json.loads轉
    c_my_json_var = json.loads(context['var']['json'].get('my_key2'))['key_in_json']
    print("c_ui_var:", c_ui_var)
    print('c_terminal_var', c_terminal_var)
    print('c_terminal_json_var', c_terminal_json_var)
    print('c_code_var', c_code_var)
    print('c_code_json_var', c_code_json_var)
    print('c_my_json_var', c_my_json_var)
    
def get_context_fun(**context):
    # 印出context所有資訊
    print("context: ", context)
    

with DAG(
    dag_id='var_dag',
    schedule_interval=None,
    start_date=datetime(2024,5,11),
    tags=['first_tag','second_tag']
) as dag:
    
    task6 = PythonOperator(
        task_id = 'task6',
        python_callable = set_var
    )
    
    task7 = PythonOperator(
        task_id = 'task7',
        python_callable = get_var
    )
    
    task8 = PythonOperator(
        task_id = 'task8',
        python_callable = get_var_by_context
    )
    
    task9 = PythonOperator(
        task_id = 'task9',
        python_callable = get_context_fun
    )
    
    task6 >> [task7, task8] >> task9
    
    
    

# TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


with DAG(
    'group_dag',
    schedule_interval=None,
    start_date=datetime(2024,5,11),
) as dag:
    with TaskGroup(group_id = 'my_task_group') as tg1:
        task10 = EmptyOperator(task_id = 'task10')
        task11 = EmptyOperator(task_id = 'task11')
        task12 = EmptyOperator(task_id = 'task12')
        
        # 在group內也要定義順序
        task10 >> task11 >> task12
        
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')
    
    start_task >> tg1 >> end_task
    



# topic: 假設拿到一筆 json 格式的訂單資料，要利用訂單金額和訂單數量計算出平均的訂單金額
# 傳統寫法, 透過xcom傳遞
def extract(ti): #用json模組讀取資料, 三個單引號 或 三個雙引號 可表示多行註解 或 可以當多行字串使用
    json_string = """
        [
            {
                "order_id": "1001",
                "order_item": "薯餅蛋餅",
                "order_price": 45
            },
            {
                "order_id": "1002",
                "order_item": "大冰奶",
                "order_price": 35
            }
        ]
    """
    order_data = json.loads(json_string)
    ti.xcom_push(key = 'order_data', value = order_data)
    print("order_data", order_data)
    
# 計算各品項的總金額
def transform_sum(ti):
    order_total = 0
    for order_dict in ti.xcom_pull(task_ids = 'extract', key = 'order_data'):
        order_total += order_dict['order_price']
    ti.xcom_push(key = 'order_total', value = order_total)
    print("order_total", order_total)
    
# 計算有多少個品項
def transfrom_count(ti):
    order_count = 0
    order_list = ti.xcom_pull(task_ids = 'extract', key = 'order_data')
    order_count += len(order_list)
    ti.xcom_push(key = 'order_count', value = order_count)
    print("order_count", order_count)
    
# 計單平均金額
def transform_average(ti):
    order_average = 0
    
    #因為task: sum, count 都是包在group裡面, 所以task_ids前面還要先加group的id
    order_total = ti.xcom_pull(task_ids = 'transform.sum', key = 'order_total')
    order_count = ti.xcom_pull(task_ids = 'transform.count', key = 'order_count')
    print("oerder_total", order_total)
    print("order_count", order_count)
    order_average = order_total/order_count #除號左右兩邊不能有空格
    ti.xcom_push(key = 'order_average', value = order_average)
    print("order_average", order_average)

# 印出平均金額
def load(ti):
    order_average = ti.xcom_pull(task_ids = 'transform.average', key = 'order_average')
    print("平均金額: ", order_average)

with DAG(
    dag_id = 'traditional_etl_dag',
    schedule_interval = None,
    start_date = datetime(2024, 5, 12),
) as dag:
    extract = PythonOperator(
        task_id = 'extract',
        python_callable=extract
    )
    
    #sum, count, average 包成一個group
    with TaskGroup(group_id = 'transform') as transform:
        sum = PythonOperator(
            task_id = 'sum',
            python_callable=  transform_sum
        )
        
        count = PythonOperator(
            task_id = 'count',
            python_callable = transfrom_count
        )
        
        average = PythonOperator(
            task_id = 'average',
            python_callable = transform_average
        )
        # 記得定義group的順序, 中括號表示sum跟count可以同時做, 且兩者都做完才能做average
        [sum, count] >> average
        
    load = PythonOperator(
        task_id = 'load',
        python_callable = load
    )
    
    extract >> transform >> load
    
    
# topic: 假設拿到一筆 json 格式的訂單資料，要利用訂單金額和訂單數量計算出平均的訂單金額
# 使用新方式, decorators
# @dag              => 取代 with DAG()
# @task             => 取代 PythonOperator
# @task_group       => 取代 with TaskGroup()
# @task.virtualenv  => python虛擬環境
# @task.docker      => docker環境

from airflow.decorators import dag, task, task_group

@dag( #沒設定dag_id, 預設會以function name為id
    schedule_interval = None,
    start_date = datetime(2024, 5, 12)
)
def taskflow_etl_dag():
    @task() #沒設定task_id, 預設會以function name為id
    def extract(): #用json模組讀取資料, 三個單引號 或 三個雙引號 可表示多行註解 或 可以當多行字串使用
        json_string = """
            [
                {
                    "order_id": "1001",
                    "order_item": "薯餅蛋餅",
                    "order_price": 45
                },
                {
                    "order_id": "1002",
                    "order_item": "大冰奶",
                    "order_price": 35
                }
            ]
        """
        order_data = json.loads(json_string)
        print("order_data", order_data)
        return order_data
    
    @task_group()
    def transform(q): #因為transform被呼叫的時候 代的參數就是extract(), 而extract會回傳order_data
        @task()
        # 計算各品項的總金額
        def transform_sum(four_json): #因為tansform_sum被呼叫的時候, 就是由transform()帶入兩個參數, 所以這裡的參數名可以隨便命名
            order_total = 0
            for order_dict in four_json:
                order_total += order_dict['order_price']
                
            print("order_total", order_total)
            return order_total

        @task()
        # 計算有多少個品項
        def transfrom_count(four_json):
            order_count = len(four_json)
            print("order_count", order_count)
            return order_count

        @task()
        # 計單平均金額
        def transform_average(x, y):
            order_average = x/y #除號左右兩邊不能有空格
            print("order_average", order_average)
            return order_average
        
                             # transform_average(x, y)
                             # q 就是 extract 回傳回來的order_data
        order_average_result = transform_average(transform_sum(q), transfrom_count(q))
        return order_average_result
    
    @task()
    def load(order_average):
        print(f'平均金額:  {order_average}')
    
    # load(q)
    load(transform(extract()))
    
taskflow_etl_dag()




import os
import glob
import time
import loguru
import pymssql
import pandas as pd
# from common.db_tools import get_conn_info
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator


def database_conn():
    # other
    # return a db connection

    # conn_args = get_conn_info("GLOBAL_MSSQLDB_DATA_ALERT")
    try:
        # conn = pymssql.connect(**conn_args)
        conn = pymssql.connect(host = "localhost", user = "sa", password = "19890729", database = "testdb")
    except Exception as e:
        print("error1", str(e))
    cursor = conn.cursor()
    return (conn, cursor)


def scan_file():
    # \\deltafileserver\tableaureport\Data Alert\<Project>\Setting
    # 0.檢查專案資料夾內是否有excel
    # 0-1.true or false in list
    scan_folder = r'\\deltafileserver\tableaureport\Data Alert'
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

        print("\ndf\n", df)
    except Exception as e:

        print("error2", str(e))
    finally:
        print("finally")
        if cur is not None: cur.close()
        if conn is not None: conn.close()
    return 'end_task'
    # pass

class File_management:
    def download_file():
        # 1.根據scanner的結果, 有檔案就download
        # 1-1.download file
        # 1-2. sys_log.download_success / sys_log.download_fail
        # 1-3. db.download_success / db.download_fail
        print("File_management.download_file")
        pass

    def read_file():
        # 2.讀取excel
        # 2-1. read file
        # 2-2. sys_log.read_success / sys_log.read_fail
        # 2-3. db.read_success / db.read_fail
        print("File_management.read_file")
        pass

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

with DAG(
    dag_id = 'Config_Module',
    schedule_interval = None,
    start_date = datetime(2024,5,11),
    catchup = False,
    tags = ['DataAlert']
) as dag:
    # scan_file = PythonOperator(
    #     task_id = 'scan_file',
    #     python_callable = scan_file
    # )
    
    scan_file = BranchPythonOperator(
        task_id = 'scan_file',
        python_callable = scan_file,
    )

    download_file = PythonOperator(
        task_id = 'download_file',
        python_callable = File_management.download_file
    )

    read_file = PythonOperator(
        task_id = 'read_file',
        python_callable = File_management.read_file
    )

    backup_file = PythonOperator(
        task_id = 'backup_file',
        python_callable = File_management.backup_file
    )

    remove_file = PythonOperator(
        task_id = 'remove_file',
        python_callable = File_management.remove_file
    )

    validation_field = PythonOperator(
        task_id = 'validation_field',
        python_callable = Validation.validation_field
    )

    validation_rule = PythonOperator(
        task_id = 'validation_rule',
        python_callable = Validation.validation_rule
    )

    # send_mail = PythonOperator(
    #     task_id = 'send_mail',
    #     python_callable = Notification.send_mail
    # )

    end_task = DummyOperator(
        task_id = 'end_task'
    )

    scan_file >> [download_file, end_task]
    download_file >> read_file >> validation_field >> [validation_rule, end_task]
    validation_rule >> backup_file >> remove_file >> end_task