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
        'JD': {'name': '首都航空', 'aircraft': 'A319/A320/A321/A332/A333'},
        'HU': {'name': '海南航空', 'aircraft': 'A20N/A21N/A332/A333/B738/B38M/B788/B789'},
        'GS': {'name': '天津航空', 'aircraft': 'A320/A20N/A321/A332/E190/E195'},
        'PN': {'name': '西部航空', 'aircraft': 'A319/A19N/A320/A20N/A321/A21N'},
        '9H': {'name': '长安航空', 'aircraft': 'B738'},
        'CN': {'name': '大新华航空', 'aircraft': 'B738'},
        'UQ': {'name': '乌鲁木齐航空', 'aircraft': 'B738'},
        'GX': {'name': '北部湾航空', 'aircraft': 'A320/A20N/E190'},
        '8L': {'name': '祥鹏航空', 'aircraft': 'A320/A20N/A333/B737/B738/B38M'},
        'FU': {'name': '福州航空', 'aircraft': 'B738/B38M'},
        'Y8': {'name': '金鹏航空', 'aircraft': 'B738'}
    }
    
    # 1. 读取数据文件
    print("读取数据文件...")
    df_flight = None
    
    # 先尝试读取md文件
    if Path("v0910.md").exists():
        df_flight = read_md_to_excel("v0910.md")
        print("成功读取md文件")
    
    # 如果md文件不存在或读取失败，读取Excel文件
    if df_flight is None:
        try:
            df_flight = pd.read_excel('v0910origin.xlsx')
            print("成功读取Excel文件")
        except:
            print("无法读取数据文件")
            return
    
    print(f"原始数据: {len(df_flight)} 行")
    
    # 2. 读取机场信息文件
    airport_mapping = {}
    try:
        df_airport = pd.read_csv('cityairport_CNname_IATA_ICAO_coords.csv', encoding='gbk')
        airport_mapping = dict(zip(df_airport['全名'], df_airport['简名(城市/机场名)']))
        print(f"机场信息: {len(airport_mapping)} 个机场")
    except:
        print("机场信息文件读取失败，将使用原始城市名")
    
    # 3. 删除适用产品列
    if '适用产品' in df_flight.columns:
        df_flight = df_flight.drop('适用产品', axis=1)
        print("已删除'适用产品'列")
    
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
    if airport_mapping:
        print("标准化机场名...")
        def find_airport_name(city_name):
            for full_name, short_name in airport_mapping.items():
                if str(city_name) in str(full_name) or str(full_name) in str(city_name):
                    return short_name
            return city_name
        
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
    output_file = 'v0910.xlsx'
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 保存完整数据到2666 sheet
        df_flight.to_excel(writer, sheet_name='2666', index=False)
        print(f"完整数据已保存到sheet '2666': {len(df_flight)} 行")
        
        # 保存夜间航班数据到666 sheet
        df_night_flight.to_excel(writer, sheet_name='666', index=False)
        print(f"夜间航班数据已保存到sheet '666': {len(df_night_flight)} 行")
    
    print(f"\n处理完成！文件已保存为: {output_file}")
    print(f"原始数据: {len(df_flight)} 行, {len(df_flight.columns)} 列")
    print(f"夜间航班: {len(df_night_flight)} 行")
    print(f"剔除的白天航班: {len(df_flight) - len(df_night_flight)} 行")
    print("列名:", list(df_flight.columns))
    print("\n完整数据前3行:")
    print(df_flight.head(3).to_string())
    print("\n夜间航班前3行:")
    print(df_night_flight.head(3).to_string())
    
    return df_flight, df_night_flight

# 主程序
if __name__ == "__main__":
    print("航线数据处理程序")
    print("=" * 30)
    process_flight_data()