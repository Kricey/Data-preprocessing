import csv
import json
from glob import glob
from os import path, makedirs


def extract_fields_from_json(json_obj):

    #从 JSON 对象中提取 post_id, msg_num, user_nickname, msg, user_id, quote_post_id 字段。
 
    extracted_data = []
    response = json_obj.get('response', {})
    item_data = response.get('item_data', [])

    for item in item_data:
        post_id = item.get('post_id', '')
        msg_num = item.get('msg_num', '')
        user_nickname = item.get('user_nickname', '')
        msg = item.get('msg', '')
        user_id = item.get('user', {}).get('user_id', '')
        quote_post_id = item.get('quote_post_id', '')

        extracted_data.append({
            'post_id': post_id,
            'msg_num': msg_num,
            'user_nickname': user_nickname,
            'msg': msg,
            'user_id': user_id,
            'quote_post_id': quote_post_id,
        })
    return extracted_data


def process_file(src_file):

    #处理 CSV 文件，提取指定的字段并保存为新的 CSV 文件。
    
    print(f"Processing file: {src_file}")
    dst_file = path.join('processed', path.basename(src_file))
    
    # 确保保存目录存在
    makedirs(path.dirname(dst_file), exist_ok=True)
    
    extracted_data_list = []
    
    # 打开源CSV文件和目标CSV文件
    with open(src_file, 'r') as csvfile, open(dst_file, 'w', newline='', encoding='utf-8') as output_csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        writer = csv.DictWriter(output_csvfile, fieldnames=["post_id", "msg_num", "user_nickname", "msg", "user_id", "quote_post_id"])
        writer.writeheader()  # 写入CSV表头
        
        for row in reader:
            try:
                # 假设第三列包含 JSON 字符串
                json_str = row[2]
                json_obj = json.loads(json_str)
                
                # 提取指定字段
                extracted_data = extract_fields_from_json(json_obj)
                extracted_data_list.extend(extracted_data)
                
                # 将提取的数据写入CSV文件
                for data in extracted_data:
                    writer.writerow(data)

            except (IndexError, ValueError, json.JSONDecodeError) as e:
                print(f"Skipping invalid line due to error: {e}")
                continue
    
    print(f"Extracted data saved to: {dst_file}")


def main():
    src_files = glob('/Users/apple/Desktop/lihkg-3775474-2840.csv')

    for src_file in src_files:
        process_file(src_file)


if __name__ == '__main__':
    main()
