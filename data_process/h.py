import pandas as pd
import folium
from folium import plugins
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
import os

def load_airport_coordinates():
    """加载机场坐标数据库"""
    coord_file = '20250912/cityairport_CNname_IATA_ICAO_coords.xlsx'
    try:
        coord_df = pd.read_excel(coord_file)
        coord_df.columns = ['city_airport', 'full_name', 'iata', 'icao', 'coordinates']
        
        coord_dict = {}
        for idx, row in coord_df.iterrows():
            city_airport = str(row['city_airport']).strip()
            coord_str = str(row['coordinates'])
            try:
                lat, lng = map(float, coord_str.split(','))
                coord_dict[city_airport] = (lat, lng)
            except:
                continue
        return coord_dict
    except Exception as e:
        print(f"读取坐标数据库失败: {e}")
        return {}

def parse_time(time_str):
    """解析时间格式，处理跨日航班"""
    if pd.isna(time_str):
        return None
    
    time_str = str(time_str).strip()
    
    # 处理时间格式 HH:MM
    try:
        if ':' in time_str:
            hour, minute = map(int, time_str.split(':'))
        else:
            # 处理数字格式
            time_str = str(int(float(time_str))) if time_str.replace('.', '').isdigit() else time_str
            if len(time_str) <= 2:  # 如 "5" -> "00:05"
                hour, minute = 0, int(time_str)
            elif len(time_str) == 3:  # 如 "815" -> "08:15"
                hour, minute = int(time_str[0]), int(time_str[1:])
            else:  # 如 "2250" -> "22:50"
                hour, minute = int(time_str[:2]), int(time_str[2:])
    except:
        return None
    
    # 处理跨日情况（小于6点认为是次日）
    is_next_day = hour < 6
    
    return {
        'hour': hour,
        'minute': minute,
        'is_next_day': is_next_day,
        'display': f"{hour:02d}:{minute:02d}" + ("次日" if is_next_day else "")
    }

def parse_coordinates(coord_str):
    """解析坐标字符串"""
    if pd.isna(coord_str):
        return None, None
    try:
        lat, lng = map(float, str(coord_str).split(','))
        return lat, lng
    except:
        return None, None

def parse_days(days_str):
    """解析航班日期"""
    if pd.isna(days_str):
        return []
    days_str = str(days_str).strip()
    return [int(d) for d in days_str if d.isdigit()]

def is_morning_flight(time_info):
    """判断是否为早班航班（4-12点起飞）"""
    if not time_info:
        return False
    return 6 <= time_info['hour'] < 12

def classify_aircraft_size(aircraft_type_str):
    """根据机型分类大小"""
    if pd.isna(aircraft_type_str):
        return "默认"
    
    aircraft_type_str = str(aircraft_type_str).strip().upper()
    
    # 机型映射表
    small_aircraft = ['E190', 'E195']
    medium_aircraft = ['A319', 'A19N', 'A320', 'A20N', 'A21N', 'A321', 'B737', 'B738', 'B38M']
    large_aircraft = ['A332', 'A333', 'B788', 'B789']
    
    # 检查是否包含各类型机型
    has_small = any(aircraft in aircraft_type_str for aircraft in small_aircraft)
    has_medium = any(aircraft in aircraft_type_str for aircraft in medium_aircraft)
    has_large = any(aircraft in aircraft_type_str for aircraft in large_aircraft)
    
    # 统计包含的尺寸类型数量
    size_types_count = sum([has_small, has_medium, has_large])
    
    if size_types_count == 0:
        return "默认"  # 无法识别的机型
    elif size_types_count == 1:
        # 只有一种尺寸类型
        if has_small:
            return "小型"
        elif has_medium:
            return "中型"
        else:
            return "大型"
    else:
        # 多种尺寸类型混合
        return "默认"

def merge_flights_to_routes(flights):
    """合并航班为航线"""
    routes = defaultdict(lambda: {
        'flights': [],
        'has_morning': False,
        'has_evening': False,
        'airlines': set(),
        'aircraft_types': set(),
        'aircraft_sizes': set(),
        'individual_aircraft': set(),  # 添加单独机型集合
        'departure': '',
        'arrival': '',
        'departure_coord': None,
        'arrival_coord': None
    })
    
    for flight in flights:
        # 创建航线键（起点-终点）
        route_key = f"{flight['departure']}-{flight['arrival']}"
        
        route = routes[route_key]
        route['flights'].append(flight)
        route['departure'] = flight['departure']
        route['arrival'] = flight['arrival']
        route['departure_coord'] = flight['departure_coord']
        route['arrival_coord'] = flight['arrival_coord']
        route['airlines'].add(flight['airline'])
        route['aircraft_types'].add(flight['aircraft_type'])
        if flight['aircraft_type'] and flight['aircraft_type'] != "未知机型":
            aircraft_parts = flight['aircraft_type'].replace('/', ' ').replace(',', ' ').split()
            route['individual_aircraft'].update(aircraft_parts)
        route['aircraft_sizes'].add(flight['aircraft_size'])
        route['individual_aircraft'].update(flight.get('individual_aircraft', []))  # 添加单独机型
        
        # 修复：正确设置航线的时段标记
        if flight['is_morning']:
            route['has_morning'] = True
        else:
            route['has_evening'] = True
    
    # 转换为列表格式
    route_list = []
    for route_key, route_data in routes.items():
        # 确定航线颜色
        if route_data['has_morning'] and route_data['has_evening']:
            color = '#CE6A85'  # 紫色（早晚班都有）
            route_type = '早晚班'
        elif route_data['has_morning']:
            color = '#FF8C61'  # 红色（早班）
            route_type = '早班'
        else:
            color = '#4E598C'  # 蓝色（晚班）
            route_type = '晚班'
        
        route_list.append({
            'route_key': route_key,
            'departure': route_data['departure'],
            'arrival': route_data['arrival'],
            'departure_coord': route_data['departure_coord'],
            'arrival_coord': route_data['arrival_coord'],
            'flights': route_data['flights'],
            'airlines': list(route_data['airlines']),
            'aircraft_types': list(route_data['aircraft_types']),
            'aircraft_sizes': list(route_data['aircraft_sizes']),
            'individual_aircraft': list(route_data['individual_aircraft']),  # 添加单独机型列表
            'color': color,
            'route_type': route_type,
            'flight_count': len(route_data['flights']),
            'has_morning': route_data['has_morning'],
            'has_evening': route_data['has_evening']
        })
    
    return route_list

def create_flight_visualization(excel_file, sheet_name=None, version_prefix="666"):
    """创建航线可视化"""
    
    # 加载坐标数据库
    coord_dict = load_airport_coordinates()
    if not coord_dict:
        print("警告：坐标数据库加载失败，可能影响可视化效果")
    
    # 读取Excel文件
    try:
        if sheet_name:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            print(f"读取工作表: {sheet_name}")
        else:
            df = pd.read_excel(excel_file)
            print("读取默认工作表")
    except Exception as e:
        print(f"读取Excel文件失败: {e}")
        return
    
    # 重命名列以便处理（新格式：航司名 航班号 出港城市 到港城市 出发时刻 到达时刻 班期 机型）
    df.columns = ['airline', 'flight_number', 'departure', 'arrival', 'departure_time', 'arrival_time', 'days', 'aircraft_type']
    
    # 处理数据
    flights = []
    airports = set()
    all_aircraft_types = set()
    all_aircraft_sizes = set()
    
    for idx, row in df.iterrows():
        # 从坐标数据库获取坐标
        departure_city = str(row['departure']).strip()
        arrival_city = str(row['arrival']).strip()
        
        dep_coord = coord_dict.get(departure_city)
        arr_coord = coord_dict.get(arrival_city)
        
        if dep_coord is None or arr_coord is None:
            print(f"警告：找不到坐标 - 出发城市: {departure_city}, 到达城市: {arrival_city}")
            continue
            
        dep_lat, dep_lng = dep_coord
        arr_lat, arr_lng = arr_coord
        
        # 解析时间
        dep_time = parse_time(row['departure_time'])
        arr_time = parse_time(row['arrival_time'])
        
        # 解析航班日期
        flight_days = parse_days(row['days'])
        
        # 处理机型数据
        aircraft_type = str(row['aircraft_type']) if not pd.isna(row['aircraft_type']) else "未知机型"
        aircraft_size = classify_aircraft_size(aircraft_type)
        
        all_aircraft_types.add(aircraft_type)
        all_aircraft_sizes.add(aircraft_size)
        
        # 拆分机型用于独立筛选
        individual_aircraft = []
        if aircraft_type and aircraft_type != "未知机型":
            individual_aircraft = aircraft_type.replace('/', ' ').replace(',', ' ').split()

        flight_info = {
            'airline': str(row['airline']),
            'flight_number': str(row['flight_number']),
            'days': flight_days,
            'departure': departure_city,
            'arrival': arrival_city,
            'departure_time': dep_time,
            'arrival_time': arr_time,
            'departure_coord': [dep_lat, dep_lng],
            'arrival_coord': [arr_lat, arr_lng],
            'aircraft_type': aircraft_type,
            'aircraft_size': aircraft_size,
            'is_morning': is_morning_flight(dep_time),
            'individual_aircraft': individual_aircraft
        }
        
        flights.append(flight_info)
        airports.add((departure_city, dep_lat, dep_lng))
        airports.add((arrival_city, arr_lat, arr_lng))
    
    # 合并航班为航线
    routes = merge_flights_to_routes(flights)

    # 创建地图
    center_lat = sum(coord[1] for coord in airports) / len(airports)
    center_lng = sum(coord[2] for coord in airports) / len(airports)
    
    # 获取所有航司
    all_airlines = list(set([flight['airline'] for flight in flights]))
    all_airlines.sort()
    
    # 排序机型
    all_aircraft_types = sorted(list(all_aircraft_types))
    all_aircraft_sizes = sorted(list(all_aircraft_sizes))
    
    # 生成HTML内容
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>HNA{version_prefix}随心飞航线可视化系统</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <style>
        :root {{
            --primary-color: #8E6289;
            --primary-hover: #E77B73;
            --background-primary: #ffffff;
            --background-secondary: rgba(250, 240, 242, 0.2);
            --text-primary: #0F0508;
            --text-secondary: #8e8e93;
            --border-color: rgba(141, 125, 129, 0.12);
            --border-color-dark: rgba(141, 125, 129, 0.24);
            --shadow-light: rgba(0, 0, 0, 0.08);
            --shadow-medium: rgba(0, 0, 0, 0.1);
            --backdrop-filter: blur(20px);
        }}
        
        @media (prefers-color-scheme: dark) {{
            :root {{
                --background-primary: #000000;
                --background-secondary: rgba(15, 5, 8, 0.2);
                --text-primary: #ffffff;
                --text-secondary: #8e8e93;
                --border-color: rgba(141, 125, 129, 0.24);
                --border-color-dark: rgba(141, 125, 129, 0.36);
                --shadow-light: rgba(0, 0, 0, 0.3);
                --shadow-medium: rgba(0, 0, 0, 0.4);
            }}
        }}
        
        * {{
            box-sizing: border-box;
            -webkit-tap-highlight-color: transparent;
        }}
        
        html, body {{
            margin: 0;
            padding: 0;
            height: 100%;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--background-primary);
            overflow: hidden;
        }}
        
        #map {{
            height: 100vh;
            width: 100vw;
        }}
        
        /* 地图暗黑模式适配 */
        @media (prefers-color-scheme: dark) {{
            /* 地图图层变暗 */
            .leaflet-layer {{
                filter: brightness(0.6) invert(1) contrast(3) hue-rotate(200deg) saturate(0.3) brightness(0.7);
            }}
            
            /* 控件暗黑化 - 简化滤镜 */
            .leaflet-control-zoom,
            .leaflet-control-layers,
            .leaflet-control-attribution {{
                background: #2c2c2c !important;
                border: 1px solid #444 !important;
                color: #ffffff !important;
                filter: none !important;
            }}
            
            /* 控件按钮样式 */
            .leaflet-control-zoom a {{
                background: #2c2c2c !important;
                color: #ffffff !important;
                border: none !important;
            }}

            .leaflet-control-layers-toggle {{
                background-color: #2c2c2c !important;
                color: #ffffff !important;
                border: none !important;
            }}

            /* 悬停效果 */
            .leaflet-control-zoom a:hover,
            .leaflet-control-layers-toggle:hover {{
                background: #404040 !important;
                color: #ffffff !important;
            }}
            
            /* 图层控制面板 */
            .leaflet-control-layers-expanded {{
                background: #2c2c2c !important;
                border: 1px solid #444 !important;
                color: #ffffff !important;
            }}
            
            /* 图层选项文字 */
            .leaflet-control-layers label,
            .leaflet-control-layers-base label,
            .leaflet-control-layers-overlays label {{
                color: #ffffff !important;
            }}
        }}
        
        /* 控制按钮样式 - 统一样式 */
        .control-btn {{
            position: absolute;
            z-index: 1003;
            width: 50px;
            height: 50px;
            border-radius: 25px;
            background: var(--background-secondary);
            backdrop-filter: var(--backdrop-filter);
            -webkit-backdrop-filter: var(--backdrop-filter);
            border: 1px solid var(--border-color);
            box-shadow: 0 8px 32px var(--shadow-medium);
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            color: var(--text-primary);
            font-weight: 600;
            font-size: 14px;
            user-select: none;
            overflow: hidden;
        }}
        
        .control-btn:hover {{
            transform: scale(1.05);
            box-shadow: 0 12px 40px var(--shadow-medium);
        }}
        
        .control-btn:active {{
            transform: scale(0.95);
        }}
        
        .filter-toggle-btn {{
            top: 40px;
            left: 40px;
        }}
        
        .filter-toggle-btn.panel-open {{
            background: var(--border-color-dark);
            color: var(--text-primary);
        }}
        
        /* 面板内的按钮样式 */
        .panel-btn {{
            width: 50px;
            height: 50px;
            border-radius: 25px;
            background: var(--border-color);
            border: none;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
            color: var(--text-primary);
            font-weight: 600;
            font-size: 14px;
            user-select: none;
            flex-shrink: 0;
        }}
        
        .panel-btn:hover {{
            background: var(--border-color-dark);
            transform: scale(1.05);
        }}
        
        .panel-btn:active {{
            transform: scale(0.95);
        }}
       
        .panel-btn[data-action="reset"] {{
            background: #E77B73 !important;
            color: white !important;
        }}
               
        /* 扩展背景层 */
        .panel-backdrop {{
            position: absolute;
            top: 20px;
            left: 20px;
            z-index: 1001;
            width: 360px;
            height: 310px;
            border-radius: 37px;
            background: var(--background-secondary);
            backdrop-filter: var(--backdrop-filter);
            -webkit-backdrop-filter: var(--backdrop-filter);
            border: 1px solid var(--border-color);
            box-shadow: 0 8px 32px var(--shadow-medium);
            opacity: 0;
            transform: scale(0);
            transform-origin: 30px 30px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            pointer-events: none;
        }}
        
        .panel-backdrop.visible {{
            opacity: 1;
            transform: scale(1);
        }}
        
        /* 悬浮控制面板样式 */
        .control-panel {{
            position: absolute;
            top: 20px;
            left: 20px;
            z-index: 1002;
            background: transparent;
            padding: 0;
            border-radius: 0;
            border: none;
            color: var(--text-primary);
            width: 360px;
            height: 310px;
            display: flex;
            flex-direction: column;
            gap: 0;
            transition: all 0.3s ease;
            overflow: hidden;
            pointer-events: none;
        }}
        
        .control-panel-content {{
            opacity: 0;
            transform: translateY(10px);
            transition: all 0.3s ease 0.1s;
            pointer-events: none;
            height: 100%;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
            padding: 20px;
        }}
        
        .control-panel.visible .control-panel-content {{
            opacity: 1;
            transform: translateY(0);
            pointer-events: all;
        }}
        
        /* 按钮行 */
        .button-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}
        
        .filter-section {{
            display: flex;
            flex-direction: column;
            gap: 12px;
            flex: 1;
        }}
        
        .filter-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            align-items: center;
        }}
        
        .filter-row.single {{
            grid-template-columns: 1fr;
        }}
        
        .control-group {{
            display: flex;
            flex-direction: column;
            gap: 0;
        }}
        
        select, input[type="text"] {{
            padding: 8px 12px;
            border: none;
            border-radius: 19px;
            background: var(--border-color);
            color: var(--text-primary);
            font-size: 14px;
            transition: all 0.2s ease;
            -webkit-appearance: none;
            appearance: none;
            min-height: 36px;
            width: 100%;
        }}
        
        select:focus, input[type="text"]:focus {{
            outline: none;
            background: rgba(0, 122, 255, 0.1);
            box-shadow: 0 0 0 2px rgba(0, 122, 255, 0.3);
        }}
        
        select {{
            background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%238e8e93' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3e%3cpolyline points='6,9 12,15 18,9'%3e%3c/polyline%3e%3c/svg%3e");
            background-repeat: no-repeat;
            background-position: right 10px center;
            background-size: 16px;
            padding-right: 36px;
        }}
        
        @media (prefers-color-scheme: dark) {{
            select {{
                background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23ffffff' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3e%3cpolyline points='6,9 12,15 18,9'%3e%3c/polyline%3e%3c/svg%3e");
            }}
        }}
        
        select option {{
            background: var(--background-primary);
            color: var(--text-primary);
            padding: 8px 12px;
        }}
        
        .checkbox-group {{
            display: flex;  /* 改用flex布局 */
            gap: 2px;  /* 减小间距 */
            margin-top: 3px;
            width: 100%;  /* 确保占满容器宽度 */
            justify-content: space-between;  /* 均匀分布 */
        }}
        
        .checkbox-item {{
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 10px 2px;  /* 减小左右内边距 */
            border-radius: 22px;
            background: var(--border-color);
            color: var(--text-secondary);
            font-size: 14px;  /* 稍微减小字体 */
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            user-select: none;
            min-height: 40px;
            flex: 1;  /* 让每个按钮等宽 */
            min-width: 0;  /* 允许按钮缩小 */
            max-width: 45px;  /* 限制最大宽度 */
        }}
        
        .checkbox-item.checked {{
            background: var(--primary-color);
            color: white;
        }}
        
        .checkbox-item input {{
            display: none;
        }}
        
        /* 优化后的图例和免责声明 - 基础样式 */
        .legend-disclaimer {{
            position: absolute;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: var(--background-secondary);
            backdrop-filter: var(--backdrop-filter);
            -webkit-backdrop-filter: var(--backdrop-filter);
            padding: 12px 16px;
            border-radius: 19px;
            box-shadow: 0 8px 32px var(--shadow-medium);
            z-index: 1000;
            font-size: 12px;
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            max-width: 90vw;
            width: max-content;
            min-width: 400px;
            transition: opacity 0.3s ease;
        }}
        
        .legend-disclaimer.faded {{
            opacity: 0.2;
        }}
        
        .legend-items-container {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
            gap: 16px;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 0;
            white-space: nowrap;
            flex: 1;
            justify-content: center;
            font-size: 12px;
        }}
        
        .legend-color {{
            width: 18px;
            height: 3px;
            margin-right: 6px;
            border-radius: 2px;
            flex-shrink: 0;
        }}
        
        .disclaimer-section {{
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid var(--border-color);
        }}
        
        .disclaimer-section p {{
            margin: 3px 0;
            line-height: 1.3;
            color: var(--text-secondary);
            font-size: 11px;
        }}
        
        .disclaimer-section a {{
            color: var(--primary-color);
            text-decoration: none;
        }}
        
        .disclaimer-section a:hover {{
            text-decoration: underline;
        }}
        
        /* 信息面板 */
        #info-panel {{
            position: absolute;
            bottom: 20px;
            left: 20px;
            background: var(--background-secondary);
            backdrop-filter: var(--backdrop-filter);
            -webkit-backdrop-filter: var(--backdrop-filter);
            padding: 20px;
            border-radius: 37px;
            box-shadow: 0 8px 32px var(--shadow-medium);
            z-index: 1010;
            width: 360px;
            font-size: 16px;
            max-height: 55vh;
            overflow-y: auto;
            display: none;
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            transform: translateX(-100%);
            opacity: 0;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        
        #info-panel.visible {{
            display: block;
            transform: translateX(0);
            opacity: 1;
        }}
        
        #info-panel.hiding {{
            transform: translateX(-100%);
            opacity: 0;
        }}
        
        .flight-item {{
            border-bottom: 1px solid var(--border-color);
            padding: 12px 0;
            margin: 6px 0;
            font-size: 14px;  /* 添加字号设置 */
            line-height: 1.5;  /* 添加行高 */
        }}
        
        .flight-item strong {{
            font-size: 16px;  /* 航班号稍大 */
        }}

        .flight-item small {{
            font-size: 14px;  /* 详细信息稍小 */
            color: var(--text-primary);
        }}

        .flight-item:last-child {{
            border-bottom: none;
        }}
        
        .close-btn {{
            position: absolute;  /* 恢复绝对定位 */
            top: 20px;          /* 统一位置 */
            right: 20px;        /* 统一位置 */
            width: 40px;        /* 统一大小 */
            height: 40px;       /* 统一大小 */
            border-radius: 40px; /* 对应调整圆角 */
            background: var(--border-color);
            color: var(--text-secondary);
            font-size: 20px;    /* 调整字号 */
            line-height: 40px;  /* 让 X 居中 */
            text-align: center;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;       /* 添加 flex 布局 */
            align-items: center; /* 垂直居中 */
            justify-content: center; /* 水平居中 */
            z-index: 10;        /* 确保在滚动内容之上 */
        }}

        .close-btn:hover {{
            background: var(--border-color-dark);
            color: var(--text-primary);
        }}
              
        /* 地图控件淡化效果 */
        .leaflet-control-zoom,
        .leaflet-control-layers {{
            transition: opacity 0.3s ease;
        }}
        
        .leaflet-control-zoom.faded,
        .leaflet-control-layers.faded {{
            opacity: 0.2;
        }}
        
        .leaflet-control-attribution {{
            font-size: 8px !important;
            line-height: 1.2 !important;
            opacity: 0.7 !important;  /* 半透明效果 */
            background: rgba(255, 255, 255, 0.8) !important;  /* 半透明背景 */
        }}
        
        /* 移动端适配优化 - 统一处理中小屏幕 */
        @media (max-width: 768px) {{
            /* 控制面板适配 */
            .panel-backdrop {{
                width: calc(100vw - 40px);
                height: 330px;
                top: 20px;
                left: 20px;
                transform-origin: 30px 30px;
            }}
            
            .control-panel {{
                top: 20px;
                left: 20px;
                width: calc(100vw - 40px);
                height: 330px;
            }}
            
            .control-panel-content {{
                padding: 20px 20px;
                gap: 10px;
            }}
            
            .button-row {{
                margin-bottom: 6px;
            }}
            
            .filter-row {{
                gap: 10px;
            }}
            
            .checkbox-group {{
                gap: 4px;
            }}
            
            /* 移动端图例优化 - 确保居中和自适应 */
            .legend-disclaimer {{
                position: fixed;
                bottom: 20px;
                left: 50%;
                right: auto;
                transform: translateX(-50%);
                padding: 8px 12px;
                font-size: 10px;
                max-width: 95vw;
                width: auto;  /* 改为auto让内容决定宽度 */
                min-width: 320px;  /* 增加最小宽度确保一行显示 */
                text-align: center;
            }}

            .legend-items-container {{
                display: flex;
                gap: 8px;
                margin-bottom: 6px;
                flex-wrap: nowrap;
                justify-content: center;
                white-space: nowrap;
            }}

            .legend-item {{
                font-size: 10px;
                flex-shrink: 1;  /* 允许适当缩小 */
                min-width: 0;  /* 允许缩小到内容宽度 */
                white-space: nowrap;  /* 保持文字不换行 */
            }}

            .legend-color {{
                width: 14px;
                height: 2px;
                margin-right: 4px;
            }}
            
            .disclaimer-section {{
                margin-top: 6px;
                padding-top: 6px;
            }}
            
            .disclaimer-section p {{
                margin: 2px 0;
                line-height: 1.2;
                font-size: 9px;
            }}
            
            /* 信息面板移动端适配 */
            #info-panel {{
                bottom: 20px;
                left: 20px;
                right: 20px;
                width: calc(100vw - 40px);
                max-height: 45vh;
                padding: 20px;
            }}
            
            /* 表单元素移动端优化 - 只调整必要属性 */
            select, input[type="text"] {{
                font-size: 16px;  /* 防止 iOS 缩放，保持这个调整 */
                min-height: 44px; /* 保持触摸友好的高度 */
                padding: 12px 16px;
            }}

            select {{
                padding-right: 44px;
            }}
        }}
        
        /* 小屏幕特殊处理 */
        @media (max-width: 480px) {{
            .checkbox-group {{
                gap: 2px;
            }}
            
            .control-panel-content {{
                padding: 20px;
                gap: 8px;
            }}
            
            /* 小屏幕图例进一步优化 */
            .legend-disclaimer {{
                padding: 6px 8px;
                font-size: 9px;
                bottom: 20px;
                left: 50%;
                right: auto;
                transform: translateX(-50%);
                max-width: 95vw;
                width: max-content;
                min-width: 260px;
            }}
            
            .legend-items-container {{
                gap: 6px;
                margin-bottom: 4px;
            }}
            
            .legend-item {{
                font-size: 9px;
            }}
            
            .legend-color {{
                width: 12px;
                height: 2px;
                margin-right: 3px;
            }}
            
            .disclaimer-section {{
                margin-top: 4px;
                padding-top: 4px;
            }}
            
            .disclaimer-section p {{
                font-size: 8px;
                margin: 1px 0;
            }}
        }}
        
        /* 超小屏幕适配 */
        @media (max-width: 360px) {{
            .legend-items-container {{
                flex-direction: column;  /* 垂直排列 */
                gap: 4px;
            }}
            
            .legend-item {{
                justify-content: flex-start;  /* 左对齐 */
                font-size: 9px;  /* 只保留图例的字号调整 */
            }}
            
            .disclaimer-section p {{
                font-size: 7px;  /* 只保留免责声明的字号调整 */
            }}
        }}
        
        /* 大屏端保持原有优化 */
        @media (min-width: 1024px) {{
            .legend-disclaimer {{
                padding: 14px 18px;
                font-size: 12px;
            }}

            .legend-items-container {{
                gap: 20px;
                margin-bottom: 20px;
            }}

            .legend-item {{
                font-size: 12px;
            }}

            .legend-color {{
                width: 18px;
                height: 3px;
                margin-right: 6px;
            }}
            
            .disclaimer-section {{
                margin-top: 10px;
                padding-top: 10px;
            }}
            
            .disclaimer-section p {{
                margin: 3px 0;
                font-size: 11px;
            }}

            #info-panel {{
                padding: 20px;
                font-size: 16px;
                max-width: 390px;
                max-height: 60vh;
            }}
            
            .flight-item {{
                padding: 12px 0;
                margin: 8px 0;
            }}           
        }}
        
        /* 航线hover效果 */
        .route-line {{
            cursor: pointer;
        }}
        
        .route-line:hover {{
            stroke-width: 6 !important;
        }}
        
        /* 修复机场tooltip样式 */
        .leaflet-tooltip {{
            background: var(--background-secondary) !important;
            backdrop-filter: var(--backdrop-filter);
            -webkit-backdrop-filter: var(--backdrop-filter);
            border: 1px solid var(--border-color) !important;
            color: var(--text-primary) !important;
            border-radius: 10px !important;
            padding: 6px 10px !important;
            font-size: 12px !important;
            font-weight: 500 !important;
            box-shadow: 0 4px 20px var(--shadow-medium) !important;
            opacity: 1 !important;
        }}
        
        .leaflet-tooltip-bottom:before {{
            border-bottom-color: var(--border-color) !important;
        }}
        
        .leaflet-tooltip-top:before {{
            border-top-color: var(--border-color) !important;
        }}
        
        .leaflet-tooltip-left:before {{
            border-left-color: var(--border-color) !important;
        }}
        
        .leaflet-tooltip-right:before {{
            border-right-color: var(--border-color) !important;
        }}
        
        .leaflet-top.leaflet-right {{
            top: 20px;
            right: 20px;
        }}
        
        @media (max-width: 768px) {{
            .leaflet-top.leaflet-right {{
                top: 20px;
                right: 20px;
            }}
        }}
    </style>
</head>

<body>
    <div id="map"></div>
    
    <!-- 筛选控制按钮 -->
    <div class="filter-toggle-btn control-btn" id="filter-toggle-btn" onclick="toggleControlPanel()">
        <span class="filter-btn-text" id="filter-btn-text">筛选</span>
    </div>
    
    <!-- 面板展开背景 -->
    <div class="panel-backdrop" id="panel-backdrop"></div>
    
    <!-- 悬浮控制面板 -->
    <div class="control-panel" id="control-panel">
        <div class="control-panel-content" id="control-panel-content">
            <!-- 按钮行：筛选和重置按钮 -->
            <div class="button-row">
                <div style="flex: 1;"></div>
                <button class="panel-btn" data-action="reset" onclick="resetFilters()">
                    <span>重置</span>
                </button>
            </div>
            
            <div class="filter-section">              
                <!-- 第一行：航司筛选、航班时段筛选 -->
                <div class="filter-row">
                    <div class="control-group">
                        <select id="airline-filter">
                            <option value="">所有航司</option>
                        </select>
                    </div>
                    
                    <div class="control-group">
                        <select id="time-period-filter">
                            <option value="">全时段</option>
                            <option value="morning">仅早班</option>
                            <option value="evening">仅晚班</option>
                        </select>
                    </div>
                </div>
                
                <!-- 第二行：机场筛选、机场方向筛选 -->
                <div class="filter-row">
                    <div class="control-group">
                        <select id="airport-filter">
                            <option value="">所有机场</option>
                        </select>
                    </div>
                    
                    <div class="control-group">
                        <select id="airport-direction">
                            <option value="">到达+出发</option>
                            <option value="departure">仅出发</option>
                            <option value="arrival">仅到达</option>
                        </select>
                    </div>
                </div>
                
                <!-- 第三行：机型筛选、机型大小筛选 -->
                <div class="filter-row">
                    <div class="control-group">
                        <select id="aircraft-type-filter">
                            <option value="">所有机型</option>
                        </select>
                    </div>
                    
                    <div class="control-group">
                        <select id="aircraft-size-filter">
                            <option value="">所有尺寸</option>
                        </select>
                    </div>
                </div>

                <!-- 星期筛选 -->
                <div class="filter-row single">
                    <div class="control-group">
                        <div class="checkbox-group" style="margin-top: 8px;">
                            <div class="checkbox-item checked" onclick="toggleDay(1)">
                                <input type="checkbox" id="day1" checked>
                                一
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(2)">
                                <input type="checkbox" id="day2" checked>
                                二
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(3)">
                                <input type="checkbox" id="day3" checked>
                                三
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(4)">
                                <input type="checkbox" id="day4" checked>
                                四
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(5)">
                                <input type="checkbox" id="day5" checked>
                                五
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(6)">
                                <input type="checkbox" id="day6" checked>
                                六
                            </div>
                            <div class="checkbox-item checked" onclick="toggleDay(7)">
                                <input type="checkbox" id="day7" checked>
                                日
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- 优化后的图例和免责声明 -->
    <div class="legend-disclaimer" id="legend-disclaimer">
        <div class="legend-items-container">
            <div class="legend-item">
                <div class="legend-color" style="background-color: #FF8C61;"></div>
                早班航线
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #4E598C;"></div>
                晚班航线
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #CE6A85;"></div>
                早晚航线
            </div>
            <div class="legend-item">
                <div style="margin-right: 10px;">✈</div>
                航线方向
            </div>
        </div>
        
        <div class="disclaimer-section">
            <p><strong>免责声明：</strong>本 html 为本地文件，航线不会更新；机场坐标整理自公开数据，可能存在错误。</p>
            <p>航班数据源于 海航官方，没有到达时刻和机型信息，仅供参考，建议配合
                <a href="https://www.kdocs.cn/l/cqy1AfiQ4Gcx" target="_blank">v0910文档</a> 食用。
            </p>
        </div>
    </div>
    
    <!-- 信息面板 -->
    <div id="info-panel">
        <div class="close-btn" onclick="hideInfoPanel()" title="关闭">&times;</div>
        <div id="info-content"></div>
    </div>
    
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-polylinedecorator@1.6.0/dist/leaflet.polylineDecorator.js"></script>
    
    <script>
        // 航班数据
        const flights = {json.dumps(flights, ensure_ascii=False, indent=2)};
        
        // 航线数据
        const routes = {json.dumps(routes, ensure_ascii=False, indent=2)};
        
        // 机场数据
        const airports = {json.dumps(list(airports), ensure_ascii=False, indent=2)};
        
        // 初始化地图
        const map = L.map('map').setView([{center_lat}, {center_lng}], 5);
        
        // 定义多个地图底图选项
        const baseMaps = {{
            // 高德大字
            "高德大字": L.tileLayer('https://webst0{{s}}.is.autonavi.com/appmaptile?style=7&x={{x}}&y={{y}}&z={{z}}',{{
                subdomains: ['1','2','3','4'],
                attribution: '© 高德地图'
            }}),

            // 高德标准
            "高德标准": L.tileLayer('https://webrd0{{s}}.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={{x}}&y={{y}}&z={{z}}', {{
                subdomains: ['1', '2', '3', '4'],
                attribution: '© 高德地图'
            }}),
        
            // 高德卫星图
            "高德卫星": L.tileLayer('https://webst0{{s}}.is.autonavi.com/appmaptile?style=6&x={{x}}&y={{y}}&z={{z}}', {{
                subdomains: ['1', '2', '3', '4'],
                attribution: '© 高德地图'
            }}),
           
            // OpenStreetMap
            "OpenStreetMap": L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '© OpenStreetMap contributors'
            }}),
        }};
        
        // 默认使用高德地图
        baseMaps["高德大字"].addTo(map);
        
        // 添加图层控制器 - 移到右侧
        const layerControl = L.control.layers(baseMaps);
        layerControl.setPosition('topright');
        layerControl.addTo(map);
        
        // 确保缩放控件也在右侧
        map.zoomControl.setPosition('topright');
        
        // 存储图层和状态
        let routeLayers = [];
        let airportMarkers = [];
        let selectedAirport = null;
        let panelVisible = false;
        let isAnimating = false;
        
        // 淡化背景元素
        function fadeBackgroundElements(shouldFade) {{
            const legendDisclaimer = document.getElementById('legend-disclaimer');
            const zoomControl = document.querySelector('.leaflet-control-zoom');
            const layerControl = document.querySelector('.leaflet-control-layers');
            
            if (shouldFade) {{
                legendDisclaimer.classList.add('faded');
                if (zoomControl) zoomControl.classList.add('faded');
                if (layerControl) layerControl.classList.add('faded');
            }} else {{
                legendDisclaimer.classList.remove('faded');
                if (zoomControl) zoomControl.classList.remove('faded');
                if (layerControl) layerControl.classList.remove('faded');
            }}
        }}
        
        // 控制面板切换函数
        function toggleControlPanel() {{
            if (isAnimating) return;
            
            const panel = document.getElementById('control-panel');
            const toggleBtn = document.getElementById('filter-toggle-btn');
            const backdrop = document.getElementById('panel-backdrop');
            
            isAnimating = true;
            
            if (!panelVisible) {{
                // 展开动画
                panelVisible = true;
                fadeBackgroundElements(true);
                
                toggleBtn.classList.add('panel-open');
                backdrop.classList.add('visible');
                
                setTimeout(() => {{
                    panel.classList.add('visible');
                    isAnimating = false;
                }}, 100);
                
            }} else {{
                // 收起动画
                panelVisible = false;
                
                panel.classList.remove('visible');
                fadeBackgroundElements(false);
                
                setTimeout(() => {{
                    backdrop.classList.remove('visible');
                    toggleBtn.classList.remove('panel-open');
                    isAnimating = false;
                }}, 100);
            }}
        }}
        
        // 星期切换
        function toggleDay(day) {{
            const checkbox = document.getElementById(`day${{day}}`);
            const item = checkbox.parentElement;
            
            checkbox.checked = !checkbox.checked;
            item.classList.toggle('checked', checkbox.checked);
            
            updateDisplay();
        }}
        
        // 隐藏信息面板
        function hideInfoPanel() {{
            const infoPanel = document.getElementById('info-panel');
            infoPanel.classList.add('hiding');
            
            setTimeout(() => {{
                infoPanel.style.display = 'none';
                infoPanel.classList.remove('hiding');
                infoPanel.classList.remove('visible');
            }}, 300);
            
            fadeBackgroundElements(panelVisible);
        }}
        
        // 显示信息面板
        function showInfoPanel(route, matchingFlights) {{
            fadeBackgroundElements(true);
            
            let flightInfo = '';
            matchingFlights.forEach(flight => {{
                const depTime = flight.departure_time ? flight.departure_time.display : '未知';
                const arrTime = flight.arrival_time ? flight.arrival_time.display : '未知';
                const dayNames = ['', '一', '二', '三', '四', '五', '六', '日'];
                const days = flight.days.map(d => dayNames[d] || d).join('');
                
                flightInfo += `
                    <div class="flight-item">
                        <strong>${{flight.airline}} ${{flight.flight_number}}</strong><br>
                        <small>时间: ${{depTime}} - ${{arrTime}}</small><br>
                        <small>执飞: 周${{days}} | ${{flight.is_morning ? '早班' : '晚班'}}</small><br>
                        <small>机型: ${{flight.aircraft_type}} (${{flight.aircraft_size}})</small>
                    </div>
                `;
            }});
            
            const info = `
                <h4>${{route.departure}} → ${{route.arrival}}</h4>
                <p><strong>航线类型：</strong>${{route.route_type}} | <strong>航班数：</strong>${{matchingFlights.length}}</p>
                <p><strong>航空公司：</strong>${{route.airlines.join(', ')}}</p>
                <p><strong>机型：</strong>${{[...new Set(route.individual_aircraft)].sort().join(', ')}}(仅供参考)</p>
                <div style="max-height: 200px; overflow-y: auto;">
                    ${{flightInfo}}
                </div>
            `;
            
            const infoPanel = document.getElementById('info-panel');
            document.getElementById('info-content').innerHTML = info;
            infoPanel.style.display = 'block';
            
            requestAnimationFrame(() => {{
                infoPanel.classList.add('visible');
            }});
        }}
        
        // 计算弧形路径点
        function calculateArcPoints(start, end, curvature = 0.3) {{
            const startLat = start[0] * Math.PI / 180;
            const startLng = start[1] * Math.PI / 180;
            const endLat = end[0] * Math.PI / 180;
            const endLng = end[1] * Math.PI / 180;
            
            const dLng = endLng - startLng;
            const bX = Math.cos(endLat) * Math.cos(dLng);
            const bY = Math.cos(endLat) * Math.sin(dLng);
            const midLat = Math.atan2(Math.sin(startLat) + Math.sin(endLat),
                Math.sqrt((Math.cos(startLat) + bX) * (Math.cos(startLat) + bX) + bY * bY));
            const midLng = startLng + Math.atan2(bY, Math.cos(startLat) + bX);
            
            const distance = Math.sqrt(Math.pow(end[0] - start[0], 2) + Math.pow(end[1] - start[1], 2));
            const offset = distance * curvature;
            
            const perpLat = -(end[1] - start[1]) / distance * offset;
            const perpLng = (end[0] - start[0]) / distance * offset;
            
            const controlLat = (midLat * 180 / Math.PI) + perpLat;
            const controlLng = (midLng * 180 / Math.PI) + perpLng;
            
            const points = [];
            const numPoints = 20;
            
            for (let i = 0; i <= numPoints; i++) {{
                const t = i / numPoints;
                const t1 = 1 - t;
                
                const lat = t1 * t1 * start[0] + 2 * t1 * t * controlLat + t * t * end[0];
                const lng = t1 * t1 * start[1] + 2 * t1 * t * controlLng + t * t * end[1];
                
                points.push([lat, lng]);
            }}
            
            return points;
        }}
        
        // 初始化控件
        function initializeControls() {{
            // 填充航司选项
            const airlines = {json.dumps(all_airlines)};
            const airlineSelect = document.getElementById('airline-filter');
            airlines.forEach(airline => {{
                const option = document.createElement('option');
                option.value = airline;
                option.textContent = airline;
                airlineSelect.appendChild(option);
            }});
            
            // 填充机场选项
            const airportSelect = document.getElementById('airport-filter');
            airports.sort((a, b) => a[0].localeCompare(b[0]));
            airports.forEach(([name, lat, lng]) => {{
                const option = document.createElement('option');
                option.value = name;
                option.textContent = name;
                airportSelect.appendChild(option);
            }});
            
            // 填充机型选项
            const allIndividualAircraft = new Set();
            routes.forEach(route => {{
                route.individual_aircraft.forEach(aircraft => {{
                    allIndividualAircraft.add(aircraft);
                }});
            }});
            const aircraftTypeSelect = document.getElementById('aircraft-type-filter');
            Array.from(allIndividualAircraft).sort().forEach(type => {{
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type;
                aircraftTypeSelect.appendChild(option);
            }});
            
            // 填充机型大小选项
            const aircraftSizes = {json.dumps(all_aircraft_sizes)};
            const aircraftSizeSelect = document.getElementById('aircraft-size-filter');
            aircraftSizes.forEach(size => {{
                const option = document.createElement('option');
                option.value = size;
                option.textContent = size;
                aircraftSizeSelect.appendChild(option);
            }});
            
            // 绑定事件
            document.querySelectorAll('select').forEach(element => {{
                element.addEventListener('change', updateDisplay);
            }});

            // 设置默认随机选择的机场
            if (airports.length > 0) {{
                const defaultAirport = airports[Math.floor(Math.random() * airports.length)][0];
                document.getElementById('airport-filter').value = defaultAirport;
                selectedAirport = defaultAirport;
            }}
        }}
        
        // 创建机场标记
        function createAirportMarkers() {{
            airportMarkers.forEach(marker => map.removeLayer(marker));
            airportMarkers = [];
            
            airports.forEach(([name, lat, lng]) => {{
                const marker = L.circleMarker([lat, lng], {{
                    radius: 8,
                    fillColor: selectedAirport === name ? '#E77B73' : '#8E6289',
                    color: '#ffffff',
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 0.8
                }}).addTo(map);
                
                marker.bindTooltip(name, {{
                    permanent: false,
                    direction: 'top',
                    offset: [0, -15],
                    opacity: 1
                }});
                
                marker.on('mouseover', function(e) {{
                    marker.setStyle({{
                        radius: 12,
                        fillOpacity: 1
                    }});
                }});
                
                marker.on('mouseout', function(e) {{
                    marker.setStyle({{
                        radius: 8,
                        fillOpacity: 0.8
                    }});
                }});
                
                marker.on('click', function(e) {{
                    e.originalEvent.stopPropagation();
                    selectedAirport = selectedAirport === name ? null : name;
                    document.getElementById('airport-filter').value = selectedAirport || '';
                    updateDisplay();
                }});
                
                airportMarkers.push(marker);
            }});
        }}
        
        // 获取当前筛选条件
        function getCurrentFilters() {{
            const selectedDays = [];
            for (let i = 1; i <= 7; i++) {{
                if (document.getElementById(`day${{i}}`).checked) {{
                    selectedDays.push(i);
                }}
            }}
            
            return {{
                days: selectedDays,
                airline: document.getElementById('airline-filter').value,
                timePeriod: document.getElementById('time-period-filter').value,
                airport: document.getElementById('airport-filter').value,
                airportDirection: document.getElementById('airport-direction').value,
                aircraftType: document.getElementById('aircraft-type-filter').value,
                aircraftSize: document.getElementById('aircraft-size-filter').value
            }};
        }}
        
        // 航线筛选逻辑
        function routeMatchesFilters(route, filters) {{
            if (filters.timePeriod) {{
                if (filters.timePeriod === 'morning' && !route.has_morning) {{
                    return false;
                }}
                if (filters.timePeriod === 'evening' && !route.has_evening) {{
                    return false;
                }}
            }}
                      
            if (filters.airport) {{
                if (filters.airportDirection === 'departure' && route.departure !== filters.airport) {{
                    return false;
                }}
                if (filters.airportDirection === 'arrival' && route.arrival !== filters.airport) {{
                    return false;
                }}
                if (!filters.airportDirection && route.departure !== filters.airport && route.arrival !== filters.airport) {{
                    return false;
                }}
            }}
            
            const matchingFlights = route.flights.filter(flight => {{
                if (!flight.days.some(day => filters.days.includes(day))) {{
                    return false;
                }}
                
                if (filters.airline && flight.airline !== filters.airline) {{
                    return false;
                }}
                
                if (filters.aircraftType) {{
                    // 检查单独机型是否包含筛选的机型
                    if (!flight.individual_aircraft || !flight.individual_aircraft.includes(filters.aircraftType)) {{
                        return false;
                    }}
                }}
                
                if (filters.aircraftSize && flight.aircraft_size !== filters.aircraftSize) {{
                    return false;
                }}
                
                if (filters.timePeriod === 'morning' && !flight.is_morning) {{
                    return false;
                }}
                if (filters.timePeriod === 'evening' && flight.is_morning) {{
                    return false;
                }}
                
                return true;
            }});
            
            return matchingFlights.length > 0;
        }}
        
        // 更新显示
        function updateDisplay() {{
            // 清除现有航线
            routeLayers.forEach(layer => map.removeLayer(layer));
            routeLayers = [];
            
            // 更新选定机场
            selectedAirport = document.getElementById('airport-filter').value || null;
            createAirportMarkers();
            
            const filters = getCurrentFilters();
            
            // 调试输出
            console.log('当前筛选条件:', filters);
            console.log('总航线数:', routes.length);
            
            // 统计各类型航线数量
            let morningCount = 0, eveningCount = 0, bothCount = 0;
            routes.forEach(route => {{
                if (route.has_morning && route.has_evening) bothCount++;
                else if (route.has_morning) morningCount++;
                else if (route.has_evening) eveningCount++;
            }});
            
            console.log(`航线统计: 仅早班 ${{morningCount}}, 仅晚班 ${{eveningCount}}, 早晚班 ${{bothCount}}`);
            
            const filteredRoutes = routes.filter(route => routeMatchesFilters(route, filters));
            
            console.log('筛选后航线数:', filteredRoutes.length);
            
            // 绘制航线
            filteredRoutes.forEach((route, index) => {{
                // 计算弧形路径
                const arcPoints = calculateArcPoints(route.departure_coord, route.arrival_coord);
                
                // 创建弧形航线
                const line = L.polyline(arcPoints, {{
                    color: route.color,
                    weight: 4,
                    opacity: 0.8,
                    className: 'route-line'
                }}).addTo(map);
                
                // 添加圆角箭头
                const decorator = L.polylineDecorator(line, {{
                    patterns: [{{
                        offset: '15%',
                        repeat: '30%',
                        symbol: L.Symbol.marker({{
                            rotate: true,
                            markerOptions: {{
                                icon: L.divIcon({{
                                    className: "arrow-icon",
                                    html: `<svg width="24" height="16" viewBox="0 0 20 10" style="overflow: visible; transform: rotate(90deg);">
                                        <path d="M0,5 Q3,5 5,3 L15,1 Q19,3 19,5 Q19,7 15,9 L5,7 Q3,5 0,5 Z" 
                                            fill="${{route.color}}" 
                                            stroke="none"/>
                                    </svg>`,
                                    iconSize: [24, 16],
                                    iconAnchor: [12, 8]
                                }})
                            }}
                        }})
                    }}]
                }}).addTo(map);
              
                // 为航线添加点击事件
                const clickHandler = function(e) {{
                    // 阻止事件冒泡到地图
                    L.DomEvent.stopPropagation(e);
                    
                    console.log('航线被点击:', route.route_key);
                    
                    // 计算符合筛选条件的航班
                    const matchingFlights = route.flights.filter(flight => {{
                        // 应用相同的筛选逻辑
                        if (!flight.days.some(day => filters.days.includes(day))) return false;
                        if (filters.airline && flight.airline !== filters.airline) return false;
                        if (filters.aircraftType && flight.aircraft_type !== filters.aircraftType) return false;
                        if (filters.aircraftSize && flight.aircraft_size !== filters.aircraftSize) return false;
                        // 时段筛选
                        if (filters.timePeriod === 'morning' && !flight.is_morning) return false;
                        if (filters.timePeriod === 'evening' && flight.is_morning) return false;
                        
                        return true;
                    }});
                    
                    // 显示信息面板
                    showInfoPanel(route, matchingFlights);
                }};
                
                // 绑定点击事件
                line.on('click', clickHandler);
                decorator.on('click', clickHandler);
                
                routeLayers.push(line);
                routeLayers.push(decorator);
            }});
            
            // 更新统计信息
            console.log(`显示 ${{filteredRoutes.length}} 条航线`);
        }}
        
        // 重置筛选
        function resetFilters() {{
            // 重置所有复选框
            document.querySelectorAll('.checkbox-item').forEach(item => {{
                item.classList.add('checked');
                item.querySelector('input').checked = true;
            }});
            
            // 重置所有下拉框
            document.querySelectorAll('select').forEach(select => select.value = '');
            
            selectedAirport = null;
            hideInfoPanel();
            updateDisplay();
        }}
        
        // 点击地图其他地方隐藏信息面板
        map.on('click', function(e) {{
            const infoPanel = document.getElementById('info-panel');
            if (infoPanel.classList.contains('visible')) {{
                infoPanel.classList.remove('visible');
                hideInfoPanel();
            }}
        }});
        
        // 阻止信息面板内的点击事件冒泡
        document.getElementById('info-panel').addEventListener('click', function(e) {{
            e.stopPropagation();
        }});
        
        // 移动端适配：防止双击缩放
        map.doubleClickZoom.disable();
        
        // 初始化
        console.log('正在初始化可视化...');
        console.log('航班数据样例:', flights.slice(0, 2));
        console.log('航线数据样例:', routes.slice(0, 2));
        initializeControls();
        updateDisplay();
        console.log('初始化完成');
    </script>
</body>
</html>
    """
    
    # 保存HTML文件
    output_file = f'HNA{version_prefix}_flight_map_v0910h.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

# 主程序
if __name__ == "__main__":
    # 统一的Excel文件路径
    excel_file = "20250912/v0910.xlsx"  # 使用新生成的文件
    
    # 处理666版本（夜间航班）- 读取666工作表
    create_flight_visualization(excel_file, sheet_name="666", version_prefix="666")
    print(f"已生成: HNA666_flight_map_v0910h.html")
    
    # 处理2666版本（全部航班）- 读取2666工作表
    create_flight_visualization(excel_file, sheet_name="2666", version_prefix="2666")
    print(f"已生成: HNA2666_flight_map_v0910h.html")

    