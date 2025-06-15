import requests
import json
import pandas as pd
from datetime import datetime, timedelta

APP_ID = "kadai6-2"
# 気象庁API エンドポイント
BASE_URL = "https://www.jma.go.jp/bosai/forecast/data/forecast"

# 地域コードと地域名の辞書
AREA_CODES = {
    "130000": "東京都",
    "140000": "神奈川県", 
    "120000": "千葉県",
    "110000": "埼玉県",
    "270000": "大阪府",
    "230000": "愛知県",
    "010000": "北海道",
    "040000": "宮城県",
    "460000": "鹿児島県",
    "470000": "沖縄県"
}

def get_weather_data(area_code):
    """
    指定した地域の天気予報データを取得
    
    Args:
        area_code (str): 地域コード
    
    Returns:
        dict: 天気予報データ
    """
    url = f"{BASE_URL}/{area_code}.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"データ取得エラー: {e}")
        return None

def parse_weather_data(data, area_name):
    """
    天気予報データを解析してDataFrameに変換
    
    Args:
        data (dict): 気象庁APIから取得した生データ
        area_name (str): 地域名
    
    Returns:
        pandas.DataFrame: 整理された天気予報データ
    """
    if not data or len(data) == 0:
        return None
    
    # 予報データを取得
    forecast_data = data[0]
    time_series = forecast_data.get('timeSeries', [])
    
    weather_list = []
    
    for series in time_series:
        areas = series.get('areas', [])
        times = series.get('timeDefines', [])
        
        for area in areas:
            area_code = area.get('area', {}).get('code', '')
            
            # 天気データ
            if 'weathers' in area:
                weathers = area['weathers']
                for i, weather in enumerate(weathers):
                    if i < len(times):
                        weather_info = {
                            '地域': area_name,
                            '地域コード': area_code,
                            '日時': times[i],
                            '天気': weather,
                            '最高気温': None,
                            '最低気温': None,
                            '降水確率': None
                        }
                        weather_list.append(weather_info)
            
            # 気温データ
            if 'temps' in area:
                temps = area['temps']
                for i, temp in enumerate(temps):
                    if i < len(weather_list):
                        if i % 2 == 0:  # 最低気温
                            weather_list[i//2]['最低気温'] = temp
                        else:  # 最高気温
                            weather_list[i//2]['最高気温'] = temp
            
            # 降水確率データ
            if 'pops' in area:
                pops = area['pops']
                for i, pop in enumerate(pops):
                    if i < len(weather_list):
                        weather_list[i]['降水確率'] = f"{pop}%"
    
    return pd.DataFrame(weather_list)

def get_multiple_areas_weather():
    """
    複数地域の天気予報データを取得
    
    Returns:
        pandas.DataFrame: 全地域の天気予報データ
    """
    all_weather_data = []
    
    for area_code, area_name in AREA_CODES.items():
        print(f"{area_name}の天気予報を取得中...")
        
        # APIからデータ取得
        weather_data = get_weather_data(area_code)
        
        if weather_data:
            # データを解析
            df = parse_weather_data(weather_data, area_name)
            
            if df is not None and not df.empty:
                all_weather_data.append(df)
                print(f"  {len(df)}件のデータを取得")
            else:
                print(f"  データの解析に失敗")
        else:
            print(f"  データの取得に失敗")
    
    # 全データを結合
    if all_weather_data:
        combined_df = pd.concat(all_weather_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

def main():
    """メイン処理"""
    print("気象庁オープンデータ取得プログラム")
    print("=" * 50)
    
    # 複数地域の天気予報データを取得
    df = get_multiple_areas_weather()
    
    if df.empty:
        print("データが取得できませんでした")
        return
    
    print("\n" + "=" * 50)
    print("取得結果")
    print("=" * 50)
    print(f"総データ件数: {len(df)}件")
    print(f"対象地域数: {df['地域'].nunique()}地域")
    
    # データの概要を表示
    print(f"\n対象地域: {', '.join(df['地域'].unique())}")
    
    # 日時データを整形
    df['日付'] = pd.to_datetime(df['日時']).dt.date
    
    # 地域別データ件数
    print(f"\n地域別データ件数:")
    area_counts = df.groupby('地域').size()
    for area, count in area_counts.items():
        print(f"  {area}: {count}件")
    
    # CSVファイルに保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_forecast_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"\nデータを保存しました: {filename}")
    
    # 結果を表示
    print("\n" + "=" * 50)
    print("天気予報データ一覧")
    print("=" * 50)
    print(df)

if __name__ == "__main__":
    main()