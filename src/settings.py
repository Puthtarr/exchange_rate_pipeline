import os
from datetime import datetime


project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

data_dir = os.path.join(project_dir, "data")
raw_data = os.path.join(data_dir, 'raw')
clean_data = os.path.join(data_dir, 'clean')
config_dir = os.path.join(project_dir, "config")
log_dir = os.path.join(project_dir, 'logging')
os.makedirs(data_dir, exist_ok=True)
os.makedirs(config_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)
os.makedirs(raw_data, exist_ok=True)
os.makedirs(clean_data, exist_ok=True)

date = datetime.now().strftime('%Y-%m-%d')