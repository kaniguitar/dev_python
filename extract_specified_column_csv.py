import pandas as pd
import os

enc = 'cp932'
def split_and_process_csv(file_name, columns):
    # CSVファイルを読み込む
    chunk_size = 50000
    chunks = pd.read_csv(file_name, usecols=columns, chunksize=chunk_size, encoding=enc)
    
    processed_files = []
    for i, chunk in enumerate(chunks):
        # 分割されたファイルの保存
        processed_file_name = f'processed_part_{i}.csv'
        chunk.to_csv(processed_file_name, index=False)
        processed_files.append(processed_file_name)
    
    # 分割されたファイルを結合
    combined_csv = pd.concat([pd.read_csv(f) for f in processed_files])
    output_file_name = os.path.join(os.path.dirname(file_name), f'extract_{os.path.basename(file_name)}')
    combined_csv.to_csv(output_file_name, index=False, encoding=enc)
    
    # 一時ファイルの削除
    for f in processed_files:
        os.remove(f)

if __name__ == '__main__':
    file_name = input('CSVファイル名を入力してください: ')
    columns_input = input('出力したいカラムをカンマ区切りで入力してください（例: id,name,age）: ')
    columns = columns_input.split(',')
    
    split_and_process_csv(file_name, columns)