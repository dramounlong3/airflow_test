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
#         op_args=['{{ ds_nodash }}']
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
    dag_id ='config_module',
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
def tansform_average(ti):
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
            python_callable = tansform_average
        )
        # 記得定義group的順序, 中括號表示sum跟count可以同時做, 且兩者都做完才能做average
        [sum, count] >> average
        
    load = PythonOperator(
        task_id = 'load',
        python_callable = load
    )
    
    extract >> transform >> load