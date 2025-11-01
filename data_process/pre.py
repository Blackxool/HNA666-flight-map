import pandas as pd
import re
from pathlib import Path
from datetime import datetime

def read_md_to_excel(input_file):
    """将航班数据文本文件转换为DataFrame"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        return None
    
    lines = content.strip().split('\n')
    
    # 找到表头行
    header_line = None
    data_start_index = 0
    
    for i, line in enumerate(lines):
        if '航班号' in line and '出港城市' in line:
            header_line = line
            data_start_index = i + 1
            break
    
    if header_line is None:
        return None
    
    # 解析表头和数据
    headers = re.split(r'\t+', header_line.strip())
    data = []
    for i in range(data_start_index, len(lines)):
        line = lines[i].strip()
        if line:
            row_data = re.split(r'\t+', line)
            if len(row_data) == len(headers):
                data.append(row_data)
    
    if not data:
        return None
    
    df = pd.DataFrame(data, columns=headers)
    for col in df.columns:
        df[col] = df[col].str.strip()
    
    return df

def filter_flight_by_time(df):
    """筛选出发时刻在1959-0801之间的航班（剔除0801-1959的航班）"""
    def is_night_flight(time_str):
        """判断是否为夜间航班（19:59-08:01之间）"""
        try:
            # 清理时间字符串，去除可能的空格和特殊字符
            time_str = str(time_str).strip()
            
            # 尝试解析不同的时间格式
            time_formats = ['%H:%M', '%H%M', '%H.%M']
            
            for fmt in time_formats:
                try:
                    time_obj = datetime.strptime(time_str, fmt)
                    hour = time_obj.hour
                    minute = time_obj.minute
                    
                    # 转换为分钟数便于比较
                    total_minutes = hour * 60 + minute
                    
                    # 19:59 = 1199分钟，08:01 = 481分钟
                    # 夜间航班：19:59-23:59 或 00:01-08:01
                    return (total_minutes >= 1199) or (total_minutes <= 481 and total_minutes >= 1)
                    
                except ValueError:
                    continue
            
            # 如果无法解析，保留该航班
            print(f"无法解析时间格式: {time_str}")
            return True
            
        except Exception as e:
            print(f"时间处理错误: {time_str}, 错误: {e}")
            return True
    
    # 应用时间筛选
    mask = df['出发时刻'].apply(is_night_flight)
    filtered_df = df[mask].copy()
    
    return filtered_df

def process_flight_data():
    """完整的航线数据处理流程"""
    
    # 航司信息字典
    airline_info = {
        'JD': {'name': '首都航空', 'aircraft': 'A319/A320/A20N/A321/A21N/A332/A333'},
        'HU': {'name': '海南航空', 'aircraft': 'A20N/A21N/A332/A333/B738/B38M/B788/B789'},
        'GS': {'name': '天津航空', 'aircraft': 'A320/A20N/A321/A332/E190/E195'},
        'PN': {'name': '西部航空', 'aircraft': 'A319/A19N/A320/A20N/A321/A21N'},
        '9H': {'name': '长安航空', 'aircraft': 'B738'},
        'CN': {'name': '大新华航空', 'aircraft': 'B738'},
        'UQ': {'name': '乌鲁木齐航空', 'aircraft': 'B738'},
        'GX': {'name': '北部湾航空', 'aircraft': 'A320/A20N/E190'},
        '8L': {'name': '祥鹏航空', 'aircraft': 'A320/A20N/A21N/A333/B737/B738/B38M'},
        'FU': {'name': '福州航空', 'aircraft': 'B738/B38M'},
        'Y8': {'name': '金鹏航空', 'aircraft': 'B738'}
    }
    
    # 1. 读取数据文件
    print("读取数据文件...")
    df_flight = None

    input_file = Path("v1029.md")   # 输入文件名

    if input_file.exists():
        df_flight = read_md_to_excel(input_file)
        print(f"成功读取文件: {input_file.name}")
    else:
        print(f"未找到文件: {input_file.name}")
        return

    print(f"原始数据: {len(df_flight)} 行")
  
    # 2. 读取机场信息文件
    airport_mapping = {}
    import chardet

    # 检测文件编码
    def detect_encoding(file_path):
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            return result['encoding']

    try:
        # 自动检测编码
        file_encoding = detect_encoding('CN266_cityairport_name_IATA_ICAO_coords.csv')
        print(f"检测到的编码: {file_encoding}")
        
        df_airport = pd.read_csv('CN266_cityairport_name_IATA_ICAO_coords.csv', encoding=file_encoding)
        airport_mapping = dict(zip(df_airport['全名'], df_airport['简名(城市/机场名)']))
        print(f"机场信息: {len(airport_mapping)} 个机场")
        
    except Exception as e:
        print(f"读取失败: {e}")
    
    # 3. 删除适用产品列
    if '产品' in df_flight.columns:
        df_flight = df_flight.drop('产品', axis=1)
        print("已删除'产品'列")
    
    # 4. 处理航司信息
    print("处理航司信息...")
    
    # 提取航司代码（前两个字符）
    def get_airline_code(flight_number):
        flight_str = str(flight_number).strip()
        return flight_str[:2] if len(flight_str) >= 2 else flight_str
    
    # 添加航司名到第一列
    airline_codes = df_flight['航班号'].apply(get_airline_code)
    airline_names = [airline_info.get(code, {}).get('name', '') for code in airline_codes]
    df_flight.insert(0, '航司名', airline_names)
    
    # 5. 标准化机场名
    print("标准化机场名...")

    def find_airport_name(city_name):
        """标准化机场名"""
        city_str = str(city_name).strip()

        # 自定义优先映射表（精确匹配优先）
        custom_mapping = {
            '遵义茅台': '遵义/茅台',
            '遵义新舟': '遵义/新舟',
            '遵义': '遵义/新舟',
            '重庆万州': '万州/五桥',
            '重庆江北': '重庆/江北',
            '重庆': '重庆/江北',
            '呼伦贝尔': '呼伦贝尔/海拉尔',
            '赣州': '赣州/黄金',
            # ...可继续扩展
        }

        # 1. 精确匹配
        if city_str in custom_mapping:
            return custom_mapping[city_str]

        # 2. 包含匹配（只在没命中时）
        for original, standard in custom_mapping.items():
            if original in city_str:
                return standard

        # 3. 机场映射表匹配
        for full_name, short_name in airport_mapping.items():
            if city_str in str(full_name) or str(full_name) in city_str:
                return short_name

        # 4. 默认返回原名
        return city_str

    # 注意：此处的函数定义应与 airport_mapping 同级缩进，不在 if 内
    df_flight['出港城市'] = df_flight['出港城市'].apply(find_airport_name)
    df_flight['到港城市'] = df_flight['到港城市'].apply(find_airport_name)
    
    # 6. 添加到达时刻列
    departure_col_index = df_flight.columns.get_loc('出发时刻')
    df_flight.insert(departure_col_index + 1, '到达时刻', '23:59')
    
    # 7. 添加机型列
    aircraft_types = [airline_info.get(code, {}).get('aircraft', '') for code in airline_codes]
    df_flight['机型'] = aircraft_types
    
    # 8. 筛选夜间航班数据
    print("筛选夜间航班（19:59-08:01之间）...")
    df_night_flight = filter_flight_by_time(df_flight)
    
    # 9. 保存到一个Excel文件的两个sheet中
    print("保存数据文件...")
    output_file = input_file.with_suffix('.xlsx')   # 自动改扩展名

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_flight.to_excel(writer, sheet_name='2666', index=False)
        df_night_flight.to_excel(writer, sheet_name='666', index=False)

    print(f"\n处理完成！文件已保存为: {output_file}")
    print(f"原始数据: {len(df_flight)} 行, {len(df_flight.columns)} 列")
    print(f"夜间航班: {len(df_night_flight)} 行")
       
    return df_flight, df_night_flight

# 主程序
if __name__ == "__main__":
    print("航线数据处理程序")
    process_flight_data()
