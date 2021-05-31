from datetime import timedelta

# Конфигурация DAG
schedule_interval = timedelta(hours=1)

# Конфигурация БД источника
source_dbhost = '192.168.1.42'

source_dbport = 5432
source_dbname = 'sky_eng_source'
source_dbuser = 'postgres'
source_dbpass = 'postgres'

source_table = 'order'
source_schema = 'public'

# Конфигурация временного файлового хранилища
temp_folder_path = r'\sky_eng'
temp_filename = 'temp_file.csv'

# Конфигурация БД DWH
dwh_dbhost = '192.168.1.42'
dwh_dbport = 5432
dwh_dbname = 'sky_eng_dwh'
dwh_dbuser = 'postgres'
dwh_dbpass = 'postgres'

temp_dwh_table = 'raw_order_temp'
temp_dwh_schema = 'public'

target_dwh_table = 'raw_order'
target_dwh_schema = 'public'

# Маппинг DWH к источнику.
# Каждый кортеж представляет собой имена полей в исходной и целевой таблице.
# Если поле присутствует только в одной из таблиц, второе значение указывается как None
mapping = (
    (None, 'id'),
    ('id', 'order_id'),
    ('student_id', 'student_id'),
    ('teacher_id', 'teacher_id'),
    ('stage', 'stage'),
    ('status', 'status'),
    (None, 'row_hash'),
    ('created_at', 'created_at'),
    ('updated_at', 'updated_at')
)
