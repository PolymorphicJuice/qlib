import os
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# 指定读取的目录和保存的目录
source_directory = 'E:/BaiduNetdiskDownload/quant/2023'
destination_directory = 'E:/BaiduNetdiskDownload/quant/2023qlib'

# 确保保存目录存在，如果不存在则创建
if not os.path.exists(destination_directory):
    os.makedirs(destination_directory)

# 定义重命名规则的函数
def rename_file(filename):
    if filename.endswith('.XSHE.csv'):
        # 提取文件名的数字部分
        new_name = 'sz' + filename.split('.')[0]
    elif filename.endswith('.XSHG.csv'):
        # 提取文件名的数字部分
        new_name = 'sh' + filename.split('.')[0]
    else:
        new_name = filename.replace('.csv', '')
    
    return new_name

# 处理单个文件的函数
def process_file(filepath):
    try:
        # 读取CSV文件
        df = pd.read_csv(filepath)
        
        # 根据命名规则生成新文件名
        filename = os.path.basename(filepath)
        new_order_book_id = rename_file(filename)
        new_filename = new_order_book_id + '.csv'
        
        # 计算保存的新文件路径
        relative_path = os.path.relpath(os.path.dirname(filepath), source_directory)  # 计算相对路径
        new_save_directory = os.path.join(destination_directory, relative_path)  # 新的保存目录
        
        # 如果保存目录不存在，则创建
        if not os.path.exists(new_save_directory):
            os.makedirs(new_save_directory)
            
        new_filepath = os.path.join(new_save_directory, new_filename)
        
        # 修改order_book_id列的值为新文件名
        df['order_book_id'] = new_order_book_id
        
        # 保存为新文件
        df.to_csv(new_filepath, index=False)
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

# 收集所有csv文件的路径
file_paths = []
for root, _, files in os.walk(source_directory):
    for filename in files:
        if filename.endswith('.csv'):
            file_paths.append(os.path.join(root, filename))

# 使用ThreadPoolExecutor和tqdm处理文件
with ThreadPoolExecutor() as executor:
    # 使用tqdm显示进度条，并在多线程中处理文件
    list(tqdm(executor.map(process_file, file_paths), total=len(file_paths), desc="Processing files"))
