import pandas as pd
import numpy as np
from dash import Dash, dcc, html, Input, Output, dash_table, html
import FinanceDataReader as fdr
from pykrx import stock
from dotenv import load_dotenv
import pymysql
import re
import base64
import unicodedata
import datetime
import csv
import json
import os



load_dotenv('profile.env')

# Database connection setup
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))
db_name = os.getenv("DB_NAME")


# 데이터를 읽어옴
financial_data_path = '1924_통합재무상태표_12월16일.csv'  # 재무상태표 파일 경로
income_statement_data_path = '1924_통합손익계산서_12월16일.csv'  # 손익계산서 파일 경로
cashflow_data_path = '통합현금흐름표.csv'  # 현금흐름표 파일 경로

financial_data = pd.read_csv(financial_data_path, low_memory=False)
income_statement_data = pd.read_csv(income_statement_data_path, low_memory=False)
cashflow_data = pd.read_csv(cashflow_data_path, low_memory=False)

# 하이픈과 NaN 값을 대체
financial_data.replace('-', np.nan, inplace=True)
income_statement_data.replace('-', np.nan, inplace=True)
cashflow_data.replace('-', np.nan, inplace=True)

# 금액 컬럼들이 문자열로 되어 있으므로 이를 숫자로 변환하고 백만 원 단위로 변환
numeric_columns_financial = ['당기', '당기 1분기말', '당기 반기말', '당기 3분기말']
numeric_columns_income = ['당기', '당기 1분기 3개월', '당기 반기 3개월', '당기 3분기 3개월', '당기 3분기 누적']
numeric_columns_cashflow = ['당기', '당기 1분기', '당기 반기', '당기 3분기']

def convert_to_numeric(value):
    if isinstance(value, str):
        value = re.sub(r'[(),]', '', value)  # 괄호와 콤마를 제거
        return pd.to_numeric(value, errors='coerce')  # 숫자로 변환
    return value

def normalize_item_name(item_name):
    if isinstance(item_name, str):
        item_name = item_name.replace(' ', '').replace('　', '')  # 모든 항목명에서 띄어쓰기를 제거
        if item_name == "당기손익-공정가치금융자산":
            return "당기손익-공정가치측정금융자산"
        elif item_name in ["비지배주주지분", 'II.비지배지분', '총포괄손익,비지배지분', ' II.비지배지분', '2. 비지배지분', '2. 비지배지분', '포괄손익, 비지배지분',
                           '         포괄손익, 비지배지분', '총 포괄손익, 비지배지분', '총 포괄손익, 비지배지분', '포괄손익, 비지배지분', '포괄손익,비지배지분',
                           '비지배지분순이익']:
            return "비지배지분"
        if re.match(r'.*비지배지분', item_name):
            return '비지베지분'
        elif item_name == "이익잉여금(결손금)":
            return "이익잉여금"
        elif item_name in ["부채와자본총계", "부채및자본총계", '자본및부채총계']:
            return "자본과부채총계"
        elif any(keyword in item_name for keyword in ['지배기업의 소유주에게 귀속되는 당기순이익', '　지배기업의 소유주에게 귀속되는 당기순이익', '지배기업의소유주에게귀속되는당기순이익',
                                                      '지배기업의소유주에게귀속되는당기순이익(손실)', '지배기업의소유주에게귀속되는당기순이익(손실)', '지배기업소유주지분순이익', '지배기업소유주지분순이익', '지배기업지분순이익',
                                                      '지배기업의소유주에게귀속되는분기순이익(손실)', '지배기업의소유주에게귀속되는반기순이익', '지배주주지분순이익', '지배주주지분순이익(손실)', '지배주주지분순이익(손실)',
                                                      '지배기업의소유주에게귀속되는당기순이익(손실)', '지배기업의 소유주에게 귀속되는 당기순이익(손실)', '지배지분순이익', '지배지분순이익(손실)', 'ⅩⅤ.지배기업지분에대한주당순이익']):
            return '지배주주순이익'
        elif item_name in ['지배기업지분', '지배기업소유주지분', '지배기업지분', '지배기업의소유주지분', '지배기업소유주', '지배기업의소유주', '지배주주지분', '지배기업의소유지분']:
            return '지배주주순이익'
        elif item_name in ['지배기업의소유주지분', '지배기업소유주지분순이익','지배기업의 소유주에게 귀속되는 당기순이익(손실)', ' 지배기업의 소유주에게 귀속되는 당기순이익(손실)', '지배기업의소유주에게귀속되는당기순이익(손실)',
                           '지배기업의소유주에게귀속되는당기순이익(손실)', '지배기업의소유주에게귀속되는당기순이익(손실)', '지배기업지분', '지배기업소유주지분', '지배기업소유주지분', '지배기업의소유주에게귀속되는당기순이익', 
                           '지배기업의 소유주에게 귀속되는 당기순이익', '　지배기업의 소유주에게 귀속되는 당기순이익']:
            return '지배주주순이익'
        elif item_name == '재고자산':
            return '유동재고자산'
        elif item_name in ['분기순이익(손실)', '분기순이익', '반기순이익', '반기순이익(손실)', 'XI.반기순이익(손실)', '당기순손익',
                           'XI.반기순이익', 'XI.분기순이익', '반기의순이익', 'XI.당기순이익', '분기의순이익', '반기순손익', 'XI.반기순이익(손실)',
                           '당기순이익(손실)', '연결반기순이익', '반기순손실', '당기순손실', '분기순손실', 'Ⅴ.당기순이익(손실)', '연결당기순이익(손실)', '당기의 순이익', '당기의순이익',
                           '당기순이익(손실)(A)', 'Ⅷ.당(분)기연결순이익', 'Ⅷ.당(반)기연결순이익', '연결당기순이익', '연결반기순이익(손실)', '연결분기순이익(손실)',
                           '분기연결순이익(손실)', '-당기순이익(손실)', '분기연결순이익(손실)', '반기연결순이익(손실)', 'Ⅷ.반기연결순이익(손실)', 'Ⅲ.영업이익(손실)', '계속영업반기순이익(손실)',
                           '당기순이익(손실)', '연결분기손이익', 'Ⅵ.당기순이익', 'Ⅵ.당기순이익', '당기순이익', 'IX.반기순이익', '분기순손익', 'IX. 분기순이익', '반기연결순이익', '당기연결순이익',
                           'X.당기순이익', 'X. 당기순이익', 'X. 반기순이익', 'X.반기순이익', 'XIII. 분기순이익', 'XIII.분기순이익', 'X. 분기순이익', 'X.분기순이익',
                           '반기기순손익', '연결분기순이익', 'ⅩⅢ.분기순이익(손실)', 'Ⅰ.분기순이익(손실)']:
            return '당기순이익'
        elif item_name in ['당기총포괄손익', '분기총포괄손익', '총포괄손익', '총포괄손익(*3)', '총포괄손익']:
            return '당기총포괄손실'
        elif item_name in ['Ⅰ.유동자산', 'I. 유동자산', 'I. 유동자산']:
            return '유동자산'
        elif item_name in ['Ⅱ.비유동자산', 'II. 비유동자산', 'II. 비유동자산']:
            return '비유동자산'
        elif item_name in ['Ⅰ.유동부채', 'I. 유동부채', 'I. 유동부채']:
            return '유동부채'
        elif item_name in ['Ⅱ.비유동부채', 'II. 비유동부채', 'II. 비유동부채']:
            return '비유동부채'
        elif item_name in ['(1)자본금']:
            return '자본금'
        elif item_name in ['(2).자본잉여금', '(2)자본잉여금']:
            return '자본잉여금'
        elif item_name in ['(4)기타포괄손익누계액']:
            return '기타포괄손익누계액'
        elif item_name in ['(5)이익잉여금']:
            return '이익잉여금'
        elif item_name in ['반기말자본', '분기말자본', '당기말자본', '분기말', '반기말', '기말', '자본총계', '당기말', '당기말잔액', '반기말잔액', '분기말잔액']:
            return '자본총계'
        elif item_name in ['XIII.총포괄이익', '총포괄손익합계', 'XⅢ.총포괄이익(손실)', 'XⅢ.총포괄손익', 'XI.총포괄손익',
                           '총포괄이익(손실)', '반기총포괄손익', '분기총포괄이익', '총포괄손익']:
            return '총포괄이익'
        elif item_name in ['Ⅴ.영업이익', '영업이익(손실)', '영업손실', 'V. 영업이익', 'V. 영업이익', 'Ⅲ.영업이익(손실)', '영업손익', '영업이익(손실)',
                           'Ⅲ.영업이익', '영업이익(손실)', 'Ⅲ.영업이익(손실)', 'Ⅳ.영업이익', 'Ⅳ.영업이익', 'IV.영업이익', 'IV. 영업이익', 'III. 영업이익', 'III.영업이익',
                           'V.영업이익', 'Ⅴ.영업이익(손실)']:
            return '영업이익'
        elif item_name in ['Ⅳ.판매비와관리비', ' IV. 판매비와관리비']:
            return '판매비와관리비'
        elif item_name in ['비지배주주포괄이익(손실)']:
            return '비지배주주포괄이익'
        elif item_name in ['Ⅱ.매출원가']:
            return '매출원가'
        elif item_name in ['Ⅲ.매출총이익', '매출총이익(손실)']:
            return '매출총이익'
        elif item_name in ['Ⅹ.법인세비용']:
            return '법인세비용'
        elif item_name in ['XII.법인세비용차감후기타포괄손익']:
            return '법인세비용차감후기타포괄손익'
        elif item_name in ['XI.반기순이익', 'XI.분기순이익', '반기의순이익', 'XI.당기순이익', '분기의순이익', '반기순손익', 'XI.반기순이익(손실)']:
            return '당기의순이익'
        elif item_name in ['수익(매출액)', 'Ⅰ.매출액', '매출', '영업수익', 'I. 매출액', 'I.매출액', '영업수익(매출액)', '매출및지분법손익', 'Ⅰ.영업수익', '이자수익(매출액)',
        '매출액(영업수익)', '매출액', 'I.영업수익', 'I. 매출', 'I.매출', '수익']:
            return '매출액'
        elif item_name in ['부체총계']:
            return '부채총계'
        elif item_name in ['자산총계']:
            return '자산총계'
        elif item_name in ['기본주당이익(손실)', '기본주당이익', '보통주기본주당이익', '기본주당반기순이익(손실)', '기본주당분기순이익(손실)', '　기본주당이익 (단위 : 원)',
                           '기본주당��익(손실) 합계', '계속영업기본주당이익(손실)', '보통주 기본주당이익', '기본주당순이익', '기본주당순이익(손실)', '기본및희석주당이익']:
            return 'EPS'
    return item_name

# 항목명 통일
financial_data['항목명'] = financial_data['항목명'].apply(normalize_item_name)
income_statement_data['항목명'] = income_statement_data['항목명'].apply(normalize_item_name)
income_statement_data['당기 3분기 누적'] = income_statement_data['당기 3분기 누적'].apply(normalize_item_name)
cashflow_data['항목명'] = cashflow_data['항목명'].apply(normalize_item_name)

# 금액을 숫자로 변환하고 백만 원 단위로 변환
for col in numeric_columns_financial:
    financial_data[col] = financial_data.apply(
        lambda row: convert_to_numeric(row[col]) / 100_000_000 if pd.notna(row[col]) and row['항목명'] != 'EPS' else convert_to_numeric(row[col]),
        axis=1
    )

for col in numeric_columns_income:
    income_statement_data[col] = income_statement_data.apply(
        lambda row: convert_to_numeric(row[col]) / 100_000_000 if pd.notna(row[col]) and row['항목명'] != 'EPS' else convert_to_numeric(row[col]),
        axis=1
    )

for col in numeric_columns_cashflow:
    cashflow_data[col] = cashflow_data.apply(
        lambda row: convert_to_numeric(row[col]) / 100_000_000 if pd.notna(row[col]) and row['항목명'] != 'EPS' else convert_to_numeric(row[col]),
        axis=1
    )

# 결산기준일에서 연도 추출
financial_data['결산연도'] = pd.to_datetime(financial_data['결산기준일'], errors='coerce').dt.year
income_statement_data['결산연도'] = pd.to_datetime(income_statement_data['결산기준일'], errors='coerce').dt.year
cashflow_data['결산연도'] = pd.to_datetime(cashflow_data['결산기준일'], errors='coerce').dt.year

# 데이터 정렬
financial_data.sort_values(by=['회사명', '보고서종류'], inplace=True)
income_statement_data.sort_values(by=['회사명', '보고서종류'], inplace=True)
cashflow_data.sort_values(by=['회사명', '보고서종류'], inplace=True)

# 재무제표명(연결재무제표, 개별재무제표) 드롭다운 옵션 추출

# 재무제표 종류 옵션 생성 (재무상태표, 손익계산서, 현금흐름표)


# 보고서종류별로 사용할 컬럼을 매핑하는 함수 (재무상태표, 손익계산서, 현금흐름표 분리)
def get_columns_by_report_type(report_type, statement_type):
    if statement_type == '재무상태표':
        if report_type == '사업보고서':
            return '당기'
        elif report_type == '3분기보고서':
            return '당기 3분기말'
        elif report_type == '반기보고서':
            return '당기 반기말'
        elif report_type == '1분기보고서':
            return '당기 1분기말'
    elif statement_type == '손익계산서':
        if report_type == '사업보고서':
            return '당기'
        elif report_type == '3분기보고서':
            return '당기 3분기 3개월'
        elif report_type == '반기보고서':
            return '당기 반기 3개월'
        elif report_type == '1분기보고서':
            return '당기 1분기 3개월'
    elif statement_type == '현금흐름표':
        if report_type == '사업보고서':
            return '당기'
        elif report_type == '3분기보고서':
            return '당기 3분기'
        elif report_type == '반기보고서':
            return '당기 반기'
        elif report_type == '1분기보고서':
            return '당기 1분기'
    return None

# 변화율 계산 함수
def calculate_change(current, previous):
    try:
        if pd.notna(current) and pd.notna(previous) and previous != 0:
            return (current - previous) / abs(previous) * 100
    except (TypeError, ValueError):
        return np.nan
    return np.nan

# 변화율에 따른 화살표와 괄호 추가, 색상 표시 함수
def format_change(change):
    if pd.notna(change):
        if change > 0:
            return f"({change:.1f}%) ▲", 'red'  # 상승: 빨간색
        elif change < 0:
            return f"({change:.1f}%) ▼", 'blue'  # 하락: 파란색
    return '-', 'black'  # 변화 없음

# Dash 앱 초기화
external_scripts = [
    {
        'src': 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js'
    }
]
app = Dash(__name__, external_scripts=external_scripts, suppress_callback_exceptions=True)


company_options = [{'label': company, 'value': company} for company in pd.concat([
    financial_data['회사명'],
    income_statement_data['회사명']
]).unique()]

# 보고서 종류 옵션 생성
report_options = [{'label': report, 'value': report} for report in pd.concat([
    financial_data['보고서종류'],
    income_statement_data['보고서종류'],
]).unique()]

# 재무제표명 옵션 생성
statement_type_options = [{'label': statement, 'value': statement} for statement in pd.concat([
    financial_data['재무제표명'],
    income_statement_data['재무제표명'],
]).unique()]

year_options = [{'label': str(year), 'value': year} for year in range(2019, 2025)]

financial_statement_options = [
    {'label': '재무상태표', 'value': '재무상태표'},
    {'label': '손익계산서', 'value': '손익계산서'},
    {'label': '현금흐름표', 'value': '현금흐름표'}
]


min_rate_change = 10
max_rate_change = 50
step_rate_change = 10




# 앱 레이아웃 정의
app.layout = html.Div([

    html.H1("2019~2024 재무 시계열 분류(전체분기)"),
    html.Hr(),
    html.Label("회사명 선택"),
    dcc.Dropdown(id='timeseries-company-dropdown', options=company_options, placeholder="회사를 선택하세요", style={'margin-bottom': '20px'}),
    html.Label("재무제표명 선택"),
    dcc.Dropdown(id='timeseries-statement-type-dropdown',options=statement_type_options, placeholder="재무제표명을 선택하세요", style={'margin-bottom': '20px'}),
    # 이곳에 출력될 데이터
    html.Div(id='timeseries-output', style={'border': '1px solid black', 'padding': '14px', 'border-radius': '8px', 'margin-top': '18px'}),
    html.Button("CSV로 저장", id="timeseries-download-csv-btn", n_clicks=0),  # 버튼을 항상 레이아웃에 추가
    dcc.Download(id="timeseries-download-dataframe-csv"),

    html.Hr(),


    # html.H1("2019 ~ 2024 재무 대시보드"),

    # html.Hr(),  # 구분선 추가
    
    # # 재무제표종류 선택 드롭다운
    # html.Label("재무제표종류 선택"),
    # dcc.Dropdown(id='financial-statement-dropdown', options=financial_statement_options, placeholder="재무제표종류를 선택하세요", style={'margin-bottom': '20px'}),

    # # 회사명 선택 드롭다운
    # html.Label("회사명 선택"),
    # dcc.Dropdown(id='company-dropdown', options=company_options, placeholder="회사를 선택하세요", style={'margin-bottom': '20px'}),
    
    # # 보고서 종류 선택 드롭다운
    # html.Label("보고서종류 선택"),
    # dcc.Dropdown(id='report-dropdown', options=report_options, placeholder="보고서를 선택하세요", style={'margin-bottom': '20px'}),

    # # 재무제표명 선택 드롭다운
    # html.Label("재무제표명 선택"),
    # dcc.Dropdown(id='statement-type-dropdown', options=statement_type_options, placeholder="재무제표명을 선택하세요", style={'margin-bottom': '20px'}),

    # # 연도 선택 드롭다운 추가
    # html.Label("연도 선택(비교연도)"),
    # dcc.Dropdown(id='year-dropdown', options=year_options, placeholder="연도를 선택하세요 (선택하지 않으면 전체 연도 표시)", style={'margin-bottom': '20px'}),


    # html.Hr(),

    # # 선택한 정보 및 테이블 표시 영역
    # html.Div(id='table-and-info', children=[
    #     html.Div(id='selected-info', style={'text-align': 'center', 'font-size': '16px', 'margin-bottom': '20px', 'white-space': 'pre-line'}),
    #     html.Div(id='financial-data-table', style={'border': '1px solid black', 'padding': '10px', 'border-radius': '10px'}),
    #     html.Div("단위 : 억원", style={'text-align': 'right', 'font-size': '12px', 'margin-top': '5px', 'color': 'gray'})  # 단위 표시
    # ], style={'border': '1px solid black', 'padding': '20px', 'border-radius': '10px'}),

    # # CSV 다운로드 버튼 추가
    # html.Button("Download Table as CSV", id="download-csv-btn", style={'margin-top': '20px'}),

    # dcc.Download(id="download-dataframe-csv"),

    # # 다운로드 버튼 추가 (PNG 저장)
    # html.Button("Download Table as PNG", id="download-btn", style={'margin-top': '20px'}),

    # html.Hr(),

    html.H1("등락률에 따른 회사 필터링"),
    html.Hr(),
    html.Label("재무제표 종류 선택"),
    dcc.Dropdown(id='independent-financial-statement-dropdown', options=financial_statement_options, placeholder="재무제표종류를 선택하세요", style={'margin-bottom': '20px'}),

    html.Label("보고서 종류 선택(*밑에 비교할 보고서를 선택할 경우 이전 보고서만 선택해야함)"),
    dcc.Dropdown(id='independent-report-dropdown', options=report_options, placeholder="보고서를 선택하세요", style={'margin-bottom': '20px'}),

    html.Label("연도 선택"),
    dcc.Dropdown(id='independent-year-dropdown', options=year_options, placeholder="연도를 선택하세요", style={'margin-bottom': '20px'}),

    html.Label("재무제표명 선택 (연결/별도)"),
    dcc.Dropdown(id='independent-statement-type-dropdown', options=statement_type_options, placeholder="연결재무제표 또는 별도재무제표를 선택하세요", style={'margin-bottom': '20px'}),

    html.Label("비교할 보고서 선택 (Optional)"),
    dcc.Dropdown(id='independent-compare-report-dropdown', options=report_options, placeholder="비교할 보고서를 선택하세요", style={'margin-bottom': '20px'}),

    html.Label("항목명 선택"),
    dcc.Dropdown(
        id='independent-item-dropdown',
        options=[
            {'label': '매출액', 'value': '매출액'},
            {'label': '영업이익', 'value': '영업이익'}, 
            {'label': '당기순이익', 'value': '당기순이익'},
            {'label': '지배주주순이익', 'value': '지배주주순이익'},
            {'label': '자산총계', 'value': '자산총계'},
            {'label': '부채총계', 'value': '부채총계'},
            {'label': '자본총계', 'value': '자본총계'}
        ],
        multi=True,
        placeholder="항목명을 선택하세요",
        style={'margin-bottom': '20px'}
    ),

    html.Label("등락 비율 선택"),
    dcc.Slider(
        id='independent-rate-change-slider',
        min=min_rate_change,  # 최소값
        max=max_rate_change,  # 최대값
        step=step_rate_change,  # 단위
        value=min_rate_change,  # 초기값
        marks={i: f'{i}%' for i in range(min_rate_change, max_rate_change + step_rate_change, step_rate_change)},  # 표시값
        tooltip={"placement": "bottom", "always_visible": True},  # 툴팁 항상 표시
    ),
    
    html.Div(id='independent-rate-change-results',
             style={'border': '1px solid black', 'padding': '20px', 'border-radius': '10px', 'margin-top': '20px'}),
    # 캡처 영역을 지정하는 div
    html.Div(id='capture-div', style={'display': 'none'}),
])

def get_stock_price(company_name):
    try:
        # 종목코드 변환
        stock_code = financial_data[financial_data['회사명'] == company_name]['종목코드'].values[0]
        stock_code = str(stock_code).zfill(6)  # 종목코드를 6자리로 맞춤

        # 종목코드에 괄호가 포함되지 않도록 수정
        stock_code = re.sub(r'\[|\]', '', stock_code)

        # 현재 날짜를 구함
        today = datetime.datetime.today().strftime('%Y-%m-%d')

        # fdr 라이브러리를 사용하여 주가 조회
        stock_data = fdr.DataReader(stock_code, today, today)
        if stock_data.empty:
            return "주가 데이터 없음"
        return stock_data['Close'].values[0]
    except IndexError:
        return "종목코드 조회 실패"
    except Exception as e:
        return f"주가 조회 중 오류 발생: {str(e)}"

def market_cap(company_name):
    try:
        stock_code = financial_data[financial_data['회사명'] == company_name]['종목코드'].values[0]
        stock_code = str(stock_code).zfill(6)
        stock_code = re.sub(r'\[|\]', '', stock_code)

        today = datetime.datetime.today().strftime('%Y%m%d')
        stock_data = stock.get_market_cap(today, today, stock_code)
        
        # 오늘 시가총액 데이터가 없는 경우 전날 데이터 가져오기
        if stock_data.empty:
            yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            stock_data = stock.get_market_cap(yesterday, yesterday, stock_code)
            if stock_data.empty:
                return '시총 데이터 없음'

        return stock_data['시가총액'].values[0]
    except IndexError:
        return "종목코드 조회 실패"
    except Exception as e:
        return f"시가총액 조회 중 오류 발생 : {str(e)}"


# 콜백 함수 정의 (선택된 정보와 테이블 모두 처리)
@app.callback(
    [Output('selected-info', 'children'),
     Output('financial-data-table', 'children'),
     Output('download-dataframe-csv', 'data'),
     Output('download-csv-btn', 'n_clicks')],
    [Input('company-dropdown', 'value'),
     Input('report-dropdown', 'value'),
     Input('statement-type-dropdown', 'value'),
     Input('financial-statement-dropdown', 'value'),
     Input('year-dropdown', 'value'),
     Input('download-csv-btn', 'n_clicks')],
    prevent_initial_call=True  # 버튼을 누르기 전에는 콜백이 실행되지 않도록 설정
)

# def format_large_number(value):
#     # 숫자를 억과 조 단위로 변환
#     if value >= 1_0000_0000_0000:  # 1조 이상
#         조 = value // 1_0000_0000_0000
#         억 = (value % 1_0000_0000_0000) // 1_0000_0000
#         if 억 > 0:
#             return f"{조}조 {억}억 원"
#         else:
#             return f"{조}조 원"
#     elif value >= 1_0000_0000:  # 1억 이상
#         억 = value // 1_0000_0000
#         return f"{억}억 원"
#     else:
#         return f"{value} 원"

def update_dashboard(selected_company, selected_report, selected_statement_type, selected_financial_statement, selected_year, n_clicks):
    selected_info_output = ""
    financial_table_output = "회사명, 보고서, 재무제표명을 선택해 주세요."
    underline_accounts_income = ["매출액", "영업이익", "당기순이익", "지배주주순이익", "영업이익률"]
    underline_accounts_financial = ["자산총계", '부채총계', "자본총계", "부채비율"]

    download_data = None  # CSV 다운로드 데이터 기본값 설정

    # 주가는 손익계산서 선택 시에만 가져옴
    stock_price = None
    market_price = None
    if selected_company and selected_financial_statement == '손익계산서':
        try:
            stock_price = get_stock_price(selected_company)
            market_price = market_cap(selected_company)
            selected_info_output += f"\n{datetime.datetime.today().strftime('%Y-%m-%d')} 기준 주가: {stock_price}원"
            selected_info_output += f"시가총액: {market_price}원"
        except Exception as e:
            selected_info_output += f"\n주가 조회 중 오류 발생: {str(e)}"
            selected_info_output += f"시총 조회 중 오류 발생: {str(e)}"

    if selected_company and selected_report and selected_statement_type and selected_financial_statement:
        selected_info_output = f"{selected_company} 주요 재무사항 (단위 : 억원)"

        if selected_financial_statement == '재무상태표':
            data = financial_data
            keywords = underline_accounts_financial  # 재무상태표 키워드
        elif selected_financial_statement == '손익계산서':
            data = income_statement_data
            keywords = underline_accounts_income  # 손익계산서 키워드
        elif selected_financial_statement == '현금흐름표':
            data = cashflow_data
            keywords = []  # 현금흐름표의 경우 필터링 없이 표시

        filtered_data = data[
            (data['회사명'].str.strip().str.lower() == selected_company.strip().lower()) & 
            (data['보고서종류'].str.strip().str.lower() == selected_report.strip().lower()) & 
            (data['재무제표명'].str.strip().str.lower() == selected_statement_type.strip().lower())]
        print(filtered_data)

        q3_data = income_statement_data[
            (income_statement_data['회사명'].str.strip().str.lower() == selected_company.strip().lower()) &
            (income_statement_data['결산연도'] == 2023) &
            (income_statement_data['재무제표명'].str.strip().str.lower() == selected_statement_type.strip().lower())
        ]
        print(q3_data)

        q4_data = income_statement_data[
            (data['회사명'].str.strip().str.lower() == selected_company.strip().lower()) &
            (data['결산연도'] == 2024) &
            (data['재무제표명'].str.strip().str.lower() == selected_statement_type.strip().lower())
        ]

        # 연도가 선택된 경우 필터링 적용
        if selected_year:
            filtered_data = filtered_data[filtered_data['결산연도'].isin([selected_year, selected_year - 1, 2023, 2024])]

        current_column = get_columns_by_report_type(selected_report, selected_financial_statement)
        if not current_column:
            return selected_info_output, "해당 보고서 종류에 대한 데이터가 없습니다.", None, n_clicks

        years = [2019, 2020, 2021, 2022, 2023, 2024, "2023년 3Q 누적", '2024년 3Q 누적']
        if selected_year:
            years = [selected_year - 1, selected_year, "2023년 3Q 누적", '2024년 3Q 누적']  # 선택된 연도와 그 이전 연도만 표시

        final_table_data = {}

        for index, row in filtered_data.iterrows():
            original_name = row['항목명'].strip()
            항목명 = normalize_item_name(original_name)  # 항목명 정규화
            print(항목명)

            # 필터링: 선택된 재무제표 종류에 따라 해당 항목이 키워드 목록에 있는지 확인
            if selected_financial_statement in ['재무상태표', '손익계산서'] and 항목명 not in keywords:
                # print(f"항목명 '{항목명}'이(가) keywords에 포함되지 않아서 제외됨")  # 디버깅 출력
                continue  # 키워드에 해당하지 않으면 스ㅈ킵

            연도 = row['결산연도']
            value = row[current_column]
            if 항목명 in final_table_data:
                if pd.notna(value):
                    final_table_data[항목명][연도] = f"{value:,.0f}"
            else:
                final_table_data[항목명] = {year: '-' for year in years}
                if pd.notna(value):
                    final_table_data[항목명][연도] = f"{value:,.0f}"

        # '2023년 3Q 누적' 데이터를 추가로 처리
        for index, row in q3_data.iterrows():
            original_name = row['항목명'].strip()
            항목명 = normalize_item_name(original_name)
            q3_value = row.get('당기 3분기 누적', np.nan)

            # '2023년 3Q 누적' 값 추가
            if 항목명 in final_table_data:
                if '2023년 3Q 누적' not in final_table_data[항목명] or final_table_data[항목명]["2023년 3Q 누적"] == '-':
                    final_table_data[항목명]["2023년 3Q 누적"] = f"{q3_value:,.0f}" if pd.notna(q3_value) else '-'
            else:
                final_table_data[항목명] = {year: '-' for year in years}
                final_table_data[항목명]["2023년 3Q 누적"] = f"{q3_value:,.0f}" if pd.notna(q3_value) else '-'

        for index, row in q4_data.iterrows():
            original_name = row['항목명'].strip()
            항목명 = normalize_item_name(original_name)
            q4_value = row.get('당기 3분기 누적', np.nan)

            # '2024년 3Q 누적' 값 추가
            if 항목명 in final_table_data:
                if '2024년 3Q 누적' not in final_table_data[항목명] or final_table_data[항목명]["2024년 3Q 누적"] == '-':
                    final_table_data[항목명]["2024년 3Q 누적"] = f"{q4_value:,.0f}" if pd.notna(q4_value) else '-'
            else:
                final_table_data[항목명] = {year: '-' for year in years}
                final_table_data[항목명]["2024년 3Q 누적"] = f"{q4_value:,.0f}" if pd.notna(q4_value) else '-'


        # 부채비율 계산 추가
        if '부채총계' in final_table_data and '자본총계' in final_table_data:
            final_table_data['부채비율'] = {year: '-' for year in years}  # 부채비율 데이터 초기화
            for year in years:
                debt_value = final_table_data['부채총계'].get(year, '-')
                asset_value = final_table_data['자본총계'].get(year, '-')
                if debt_value != '-' and asset_value != '-' and float(asset_value.replace(',', '')) != 0:
                    try:
                        debt_ratio = (float(debt_value.replace(',', '')) / float(asset_value.replace(',', ''))) * 100
                        final_table_data['부채비율'][year] = f"{debt_ratio:.0f}%"
                    except (ValueError, TypeError):
                        final_table_data['부채비율'][year] = '-'

            # 변화율 계산 및 적용
            latest_year = max([year for year in years if year in final_table_data['부채비율'] and final_table_data['부채비율'][year] != '-'])
            previous_year = latest_year - 1

            if (latest_year in final_table_data['부채비율'] and previous_year in final_table_data['부채비율']
                    and final_table_data['부채비율'][latest_year] != '-' and final_table_data['부채비율'][previous_year] != '-'):
                try:
                    current_ratio = float(final_table_data['부채비율'][latest_year].replace('%', '').replace(',', ''))
                    previous_ratio = float(final_table_data['부채비율'][previous_year].replace('%', '').replace(',', ''))
                    change = calculate_change(current_ratio, previous_ratio)
                    formatted_change, color = format_change(change)
                    final_table_data['부채비율'][latest_year] += f" {formatted_change}"
                except (ValueError, TypeError):
                    pass  # 변화율 계산 중 오류가 발생하면 무시

        # 테이블 헤더 생성
        table_header = [
            html.Thead(html.Tr([
                html.Th("항목명", style={'border': 'none', 'padding': '6px', 'text-align': 'center', 'font-size': '14px', 'background-color': 'lightgray'}),
                *[html.Th(f"{year}년" if isinstance(year, int) else year, style={'border': 'none', 'padding': '6px', 'text-align': 'center', 'font-size': '14px', 'background-color': 'lightgray'}) for year in years]
            ]))
        ]

        table_body = []
        csv_data = []

        # 손익계산서인 경우 매출액 -> 영업이익 -> 당기순이익 순서로 정렬
        if selected_financial_statement == '손익계산서':
            ordered_items = ["매출액", "영업이익", "당기순이익", '지배주주순이익']
            for 항목명 in ordered_items:
                if 항목명 in final_table_data:
                    year_data = final_table_data[항목명]

                    # 가장 최근의 두 해 추출
                    available_years = [year for year in sorted(year_data.keys(), key=lambda x: (isinstance(x, int), x), reverse=True) if year_data[year] != '-']
                    latest_two_years = available_years[:2] if len(available_years) >= 2 else []
                    if len(latest_two_years) < 2:
                        continue  # 두 해 이상의 값이 없으면 등락률 계산 생략

                    # 행 생성
                    row_data = [html.Td(
                        항목명,
                        style={
                            'border': 'none',
                            'padding': '6px',
                            'font-weight': 'bold',
                            'background-color': 'lightgray'
                        }
                    )]
                    csv_row = [항목명]

                    for year in years:
                        current_value = year_data.get(year, '-')
                        csv_row.append(current_value)

                        # 가장 최근 연도와 그 이전 연도의 등락률 계산
                        if year == latest_two_years[0]:  # 최신 연도
                            previous_year = latest_two_years[1]
                            previous_value = year_data.get(previous_year, '-')

                            if current_value != '-' and previous_value != '-':
                                try:
                                    current_float = float(current_value.replace(',', ''))
                                    previous_float = float(previous_value.replace(',', ''))

                                    # 음수에서 양수 또는 양수에서 음수로 변했을 때의 등락률 계산
                                    if previous_float != 0:
                                        if previous_float < 0 < current_float or previous_float > 0 > current_float:
                                            change = ((current_float - previous_float) / abs(previous_float)) * 100
                                        else:
                                            change = ((current_float - previous_float) / abs(previous_float)) * 100
                                    else:
                                        change = np.nan 

                                    # 등락률 포맷팅
                                    formatted_change, color = format_change(change)
                                except ValueError:
                                    formatted_change, color = '-', 'black'
                            else:
                                formatted_change, color = '-', 'black'

                            row_data.append(html.Td(
                                f"{current_value} {formatted_change}",
                                style={
                                    'border': 'none',
                                    'padding': '6px',
                                    'text-align': 'right',
                                    'color': color
                                }
                            ))
                        else:
                            row_data.append(html.Td(
                                current_value,
                                style={
                                    'border': 'none',
                                    'padding': '6px',
                                    'text-align': 'right'
                                }
                            ))

                    table_body.append(html.Tr(row_data))
                    csv_data.append(csv_row)

        # 재무상태표의 경우 기존 정렬 방식 유지하며 최근 두 해 등락률 계산 추가
        elif selected_financial_statement == '재무상태표':
            for 항목명, year_data in final_table_data.items():
                # 가장 최근의 두 해 추출
                available_years = [year for year in sorted(year_data.keys(), key=lambda x: (isinstance(x, int), x), reverse=True) if year_data[year] != '-']
                latest_two_years = available_years[:2] if len(available_years) >= 2 else []
                if len(latest_two_years) < 2:
                    continue  # 두 해 이상의 값이 없으면 등락률 계산 생략

                # 행 생성
                row_data = [html.Td(
                    항목명,
                    style={
                        'border': 'none',
                        'padding': '6px',
                        'font-weight': 'bold',
                        'background-color': 'lightgray'
                    }
                )]
                csv_row = [항목명]

                for year in years:
                    current_value = year_data.get(year, '-')
                    csv_row.append(current_value)

                    # 가장 최근 연도와 그 이전 연도의 등락률 계산
                    if year == latest_two_years[0]:  # 최신 연도
                        previous_year = latest_two_years[1]
                        previous_value = year_data.get(previous_year, '-')

                        if current_value != '-' and previous_value != '-':
                            try:
                                current_float = float(current_value.replace(',', ''))
                                previous_float = float(previous_value.replace(',', ''))

                                # 음수에서 양수 또는 양수에서 음수로 변했을 때의 등락률 계산
                                if previous_float != 0:
                                    if previous_float < 0 < current_float or previous_float > 0 > current_float:
                                        change = ((current_float - previous_float) / abs(previous_float)) * 100
                                    else:
                                        change = ((current_float - previous_float) / abs(previous_float)) * 100
                                else:
                                    change = np.nan 

                                # 등락률 포맷팅
                                formatted_change, color = format_change(change)
                            except ValueError:
                                formatted_change, color = '-', 'black'
                        else:
                            formatted_change, color = '-', 'black'

                        row_data.append(html.Td(
                            f"{current_value} {formatted_change}",
                            style={
                                'border': 'none',
                                'padding': '6px',
                                'text-align': 'right',
                                'color': color
                            }
                        ))
                    else:
                        row_data.append(html.Td(
                            current_value,
                            style={
                                'border': 'none',
                                'padding': '6px',
                                'text-align': 'right'
                            }
                        ))

                table_body.append(html.Tr(row_data))
                csv_data.append(csv_row)

        # 손익계산서 선택 시 주가를 테이블 하단에 추가
        if selected_financial_statement == '손익계산서' and stock_price and market_price:
            table_body.append(html.Tr([
                html.Td(
                    f"\n{datetime.datetime.today().strftime('%m월 %d일')} 기준 주가", 
                    style={
                        'border': 'none', 
                        'padding': '6px', 
                        'text-align': 'left', 
                        'font-weight': 'bold',  
                        'font-size': '11px'
                    }
                ),
                html.Td(
                    f"{stock_price} 원", 
                    colSpan=len(years), 
                    style={
                        'border': 'none', 
                        'padding': '6px', 
                        'text-align': 'left', 
                        'font-weight': 'bold',  
                        'font-size': '11px'
                    }
                )
            ]))
            try:
                if market_price >= 1_0000_0000_0000:  # 1조 이상
                    조 = market_price // 1_0000_0000_0000
                    억 = (market_price % 1_0000_0000_0000) // 1_0000_0000
                    formatted_market_price = f"{조}조 {억}억 원" if 억 > 0 else f"{조}조 원"
                elif market_price >= 1_0000_0000:  # 1억 이상
                    억 = market_price // 1_0000_0000
                    formatted_market_price = f"{억}억 원"
                else:
                    formatted_market_price = f"{market_price} 원"
            except (TypeError, ValueError):
                # 오류가 발생할 경우 예외 처리
                formatted_market_price = "데이터 불러오기 실패"

            # 시가총액 행
            table_body.append(html.Tr([
                html.Td(
                    "\n시가총액", 
                    style={
                        'border': 'none', 
                        'padding': '6px', 
                        'text-align': 'left', 
                        'font-weight': 'bold',  
                        'font-size': '11px'
                    }
                ),
                html.Td(
                    formatted_market_price, 
                    colSpan=len(years), 
                    style={
                        'border': 'none', 
                        'padding': '6px', 
                        'text-align': 'left', 
                        'font-weight': 'bold',  
                        'font-size': '11px'
                    }
                )
            ]))

        financial_table_output = html.Table(
            children=table_header + [html.Tbody(table_body)],
            style={'border-collapse': 'collapse', 'width': '100%', 'margin': '20px 0', 'border': 'none'}
        )

        if n_clicks and n_clicks > 0:
            # CSV 파일에 기록할 헤더 (주가 칼럼은 추가하지 않음)
            csv_string = "항목명," + ",".join([f'"{year}"' for year in years]) + "\n"
            
            # 데이터 행 추가
            for row in csv_data:
                formatted_row = [f'"{item}"' if isinstance(item, str) and ',' in item else item for item in row]
                csv_string += ",".join([str(i) for i in formatted_row]) + "\n"

            # 주가 행 추가
            if selected_financial_statement == '손익계산서' and stock_price:
                csv_string += f"{datetime.datetime.today().strftime('%m월 %d일')} 기준 주가, " + "," * (len(years) - 8) + f'{stock_price} 원\n'
                csv_string += f"시가총액, " + "," * (len(years) -8) + f'{formatted_market_price}\n'

            bom = '\ufeff'
            if selected_financial_statement == '손익계산서':
                filename = f"{selected_company} 주요 손익계산서 (단위 : 억원).csv"
            else:
                filename = f"{selected_company} 주요 재무사항 (단위 : 억원).csv"
            download_data = dict(content=bom + csv_string, filename=filename)
            n_clicks = 0

    return selected_info_output, financial_table_output, download_data, n_clicks


@app.callback(
    [Output('timeseries-output', 'children'), 
     Output("timeseries-download-dataframe-csv", "data")],
    [Input('timeseries-company-dropdown', 'value'), 
     Input('timeseries-statement-type-dropdown', 'value'),  # 재무제표명 드롭다운 입력 추가
     Input("timeseries-download-csv-btn", "n_clicks")],
     prevent_initial_call=True
)

def update_timeseries_output(selected_company, selected_statement_type, n_clicks):
    if not selected_company or not selected_statement_type:
        return "회사를 선택하고 재무제표명을 선택하세요.", None

    # Connect to the database
    connection = pymysql.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
        charset='utf8'
    )

    # SQL query
    sql = f"""
    WITH 차입금_합계 AS (
    SELECT 
        보고서종류,
        결산기준일,
        결산월,
        SUM(CASE 
            WHEN 보고서종류 = '사업보고서' THEN 당기
            WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
            WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
            WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
        END) AS 총차입금
    FROM BalanceSheet
    WHERE 회사명 = '{selected_company}'
        AND 재무제표명 = '{selected_statement_type}'
        AND (
            (항목명 LIKE '%리스%' 
            OR 항목명 LIKE '%차입금%' 
            OR 항목명 LIKE '%금융부채%' 
            OR 항목명 LIKE '%사채%'
            OR 항목명 LIKE '%차입%')
            AND 항목명 NOT LIKE '%비금융부채%'
        )
    GROUP BY 보고서종류, 결산기준일, 결산월
    )
    SELECT 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = 12 THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '19.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 12 THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                    END
                END), 0) AS '20.4Q',        
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 12 THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                    END
                END), 0) AS '21.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 12 THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                    END
                END), 0) AS '22.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 12 THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                    END
                END), 0) AS '23.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 3 THEN 
                    CASE 
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '20.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 6 THEN 
                    CASE 
                        WHEN 보고서종류 = '반기보고서' AND 당기_반기말 IS NOT NULL THEN 당기_반기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                    END
                END), 0) AS '20.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 9 THEN 
                    CASE 
                        WHEN 보고서종류 = '3분기보고서' AND 당기_3분기말 IS NOT NULL THEN 당기_3분기말
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                    END
                END), 0) AS '20.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 3 THEN 
                    CASE 
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '21.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 6 THEN 
                    CASE 
                        WHEN 보고서종류 = '반기보고서' AND 당기_반기말 IS NOT NULL THEN 당기_반기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                    END
                END), 0) AS '21.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 9 THEN 
                    CASE 
                        WHEN 보고서종류 = '3분기보고서' AND 당기_3분기말 IS NOT NULL THEN 당기_3분기말
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                    END
                END), 0) AS '21.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 3 THEN 
                    CASE 
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '22.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 6 THEN 
                    CASE 
                        WHEN 보고서종류 = '반기보고서' AND 당기_반기말 IS NOT NULL THEN 당기_반기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                    END
                END), 0) AS '22.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 9 THEN 
                    CASE 
                        WHEN 보고서종류 = '3분기보고서' AND 당기_3분기말 IS NOT NULL THEN 당기_3분기말
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                    END
                END), 0) AS '22.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 3 THEN 
                    CASE 
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '23.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 6 THEN 
                    CASE 
                        WHEN 보고서종류 = '반기보고서' AND 당기_반기말 IS NOT NULL THEN 당기_반기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                    END
                END), 0) AS '23.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 
                    CASE 
                        WHEN 보고서종류 = '3분기보고서' AND 당기_3분기말 IS NOT NULL THEN 당기_3분기말
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                    END
                END), 0) AS '23.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 3 THEN 
                    CASE 
                        WHEN 보고서종류 = '1분기보고서' AND 당기_1분기말 IS NOT NULL THEN 당기_1분기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END),0) AS '24.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 6 THEN 
                    CASE 
                        WHEN 보고서종류 = '반기보고서' AND 당기_반기말 IS NOT NULL THEN 당기_반기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                    END
                END),0) AS '24.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 
                    CASE 
                        WHEN 보고서종류 = '3분기보고서' AND 당기_3분기말 IS NOT NULL THEN 당기_3분기말
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                    END
                END),0) AS '24.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN
                    CASE
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_누적
                    END
                END), 0) AS '23.3Q 누적',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN
                    CASE
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_누적
                    END
                END), 0) AS '24.3Q 누적'
    FROM IncomeStatement
    WHERE 회사명 = '{selected_company}'
        AND 재무제표명 = '{selected_statement_type}'
        AND 항목명 IN ('매출액', '영업이익', '당기순이익', '지배주주순이익')
    GROUP BY 항목명
    
    UNION ALL
    SELECT 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = 12 THEN
            CASE
                WHEN 보고서종류 = '사업보고서' THEN 당기
                WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
            END
        END), 0) AS '19.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 12 THEN
            CASE
                WHEN 보고서종류 = '사업보고서' THEN 당기
            END
        END), 0) AS '20.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 12 THEN
            CASE
                WHEN 보고서종류 = '사업보고서' THEN 당기
            END
        END), 0) AS '21.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 12 THEN
            CASE
                WHEN 보고서종류 = '사업보고서' THEN 당기
            END
        END), 0) AS '22.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 12 THEN
            CASE
                WHEN 보고서종류 = '사업보고서' THEN 당기
            END
        END), 0) AS '23.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 3 THEN 
                CASE 
                    WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                END
        END), 0) AS '20.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 6 THEN 
                CASE 
                    WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                END
        END), 0) AS '20.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '20.3Q',        
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 3 THEN 
                CASE 
                    WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                END
        END), 0) AS '21.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 6 THEN 
                CASE 
                    WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                END
        END), 0) AS '21.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '21.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 3 THEN 
                CASE 
                    WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                END
        END), 0) AS '22.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 6 THEN 
                CASE 
                    WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                END
        END), 0) AS '22.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '22.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 3 THEN 
                CASE 
                    WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                END
        END), 0) AS '23.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 6 THEN 
                CASE 
                    WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                END
        END), 0) AS '23.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '23.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 3 THEN 
                CASE 
                    WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                END
        END), 0) AS '24.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 6 THEN 
                CASE 
                    WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                END
        END), 0) AS '24.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '24.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '23.3Q 누적',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 
                CASE 
                    WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                END
        END), 0) AS '24.3Q 누적'
    FROM BalanceSheet
    WHERE 회사명 = '{selected_company}'
        AND 재무제표명 = '{selected_statement_type}'
        AND 항목명 IN ('자산총계', '부채총계', '자본총계', '현금및현금성자산')
    GROUP BY 항목명
    UNION ALL

    SELECT '총차입금' AS 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = 12 THEN 총차입금 END), 0) AS '19.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 12 THEN 총차입금 END), 0) AS '20.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 12 THEN 총차입금 END), 0) AS '21.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 12 THEN 총차입금 END), 0) AS '22.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 12 THEN 총차입금 END), 0) AS '23.4Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 3 THEN 총차입금 END), 0) AS '20.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 6 THEN 총차입금 END), 0) AS '20.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = 9 THEN 총차입금 END), 0) AS '20.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 3 THEN 총차입금 END), 0) AS '21.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 6 THEN 총차입금 END), 0) AS '21.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = 9 THEN 총차입금 END), 0) AS '21.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 3 THEN 총차입금 END), 0) AS '22.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 6 THEN 총차입금 END), 0) AS '22.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = 9 THEN 총차입금 END), 0) AS '22.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 3 THEN 총차입금 END), 0) AS '23.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 6 THEN 총차입금 END), 0) AS '23.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 총차입금 END), 0) AS '23.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 3 THEN 총차입금 END), 0) AS '24.1Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 6 THEN 총차입금 END), 0) AS '24.2Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 총차입금 END), 0) AS '24.3Q',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 총차입금 END), 0) AS '23.3Q 누적',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 총차입금 END), 0) AS '24.3Q 누적'
    FROM 차입금_합계
    ORDER BY FIELD(항목명, '매출액', '영업이익', '당기순이익', '지배주주순이익', '자산총계', '부채총계', '자본총계', '총차입금', '현금및현금성자산');
    """

    # Execute the query and convert the result to a DataFrame
    try:
        df = pd.read_sql(sql, connection)
        connection.close()

        ind_stock_price = get_stock_price(selected_company)
        ind_market_price = market_cap(selected_company)

        # Convert numeric columns to numeric values, removing commas
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
        
        profit_margin_row = []
        # Calculate 영업이익률 if 매출액 and 영업이익 exist in 항목명
        if '매출액' in df['항목명'].values and '영업이익' in df['항목명'].values:
            try:
                # Select rows for 매출액 and 영업이익
                sales_row = df[df['항목명'] == '매출액'].iloc[0, 1:]
                operating_income_row = df[df['항목명'] == '영업이익'].iloc[0, 1:]

                # Calculate 영업이익률 and round down to integer percentage
                profit_margin_row = (operating_income_row / sales_row * 100).apply(
                    lambda x: f"{int(x)}%" if pd.notna(x) and x != 0 else '-'
                )
            except Exception:
                profit_margin_row = ['-'] * (len(df.columns) - 1)
        capital_row = []
        # Calculate 영업이익률 if 매출액 and 영업이익 exist in 항목명
        if '부채총계' in df['항목명'].values and '자본총계' in df['항목명'].values:
            try:
                # Select rows for 매출액 and 영업이익
                ddept_row = df[df['항목명'] == '부채총계'].iloc[0, 1:]
                total_capital_row = df[df['항목명'] == '자본총계'].iloc[0, 1:]

                # Calculate 영업이익률 and round down to integer percentage
                capital_row = (ddept_row / total_capital_row * 100).apply(
                    lambda x: f"{int(x)}%" if pd.notna(x) and x != 0 else '-'
                )
            except Exception:
                capital_row = ['-'] * (len(df.columns) - 1)

        debt_row = []
        if '총차입금' in df['항목명'].values and '현금및현금성자산' in df['항목명'].values:
            try:
                all_debt_row = df[df['항목명'] == '총차입금'].iloc[0,1:]
                cash_row = df[df['항목명'] == '현금및현금성자산'].iloc[0,1:]

                debt_row = (all_debt_row - cash_row).apply(
                    lambda x: f"{int(x):,.0f}" if pd.notna(x) and x != 0 else '-'
                )
            except Exception:
                debt_row = ['-'] * (len(df.columns)- 1)

        # Format numeric values with commas for display
        for col in df.columns[1:]:
            df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) and isinstance(x, (int, float)) else '-')

        # Format market cap for display
        try:
            if ind_market_price >= 1_0000_0000_0000:  # 1조 이상
                조 = ind_market_price // 1_0000_0000_0000
                억 = (ind_market_price % 1_0000_0000_0000) // 1_0000_0000
                ind_formatted_market_price = f"{조}조 {억}억 원" if 억 > 0 else f"{조}조 원"
            elif ind_market_price >= 1_0000_0000:  # 1억 이상
                억 = ind_market_price // 1_0000_0000
                ind_formatted_market_price = f"{억}억 원"
            else:
                ind_formatted_market_price = f"{ind_market_price} 원"
        except (TypeError, ValueError):
            ind_formatted_market_price = "데이터 불러오기 실패"

        today_date = datetime.datetime.today().strftime('%m월 %d일')

        # Prepare HTML table output
        table_output = html.Div([
            html.H5(f"{selected_company} - 재무 시계열 분류 (단위: 억원)"),
            html.Div([  # Scrollable container
                html.Table([
                    html.Thead(html.Tr([html.Th(col) for col in df.columns], style={'background-color': 'lightgray', 'font-weight': 'bold', 'text-align': 'center'})),
                    html.Tbody([
                        html.Tr([
                            html.Td(df.iloc[i][col], style={'padding': '8px', 'text-align': 'center'}) for col in df.columns
                        ]) for i in range(len(df))
                    ] + [
                        # Add 영업이익률 row
                        html.Tr([
                            html.Td("순차입금", style={'text-align': 'center', 'padding': '8px'}),
                            *[html.Td(debt_row[i], style={'text-align': 'center', 'padding': '8px'}) for i in range(len(debt_row))]
                        ]),
                        html.Tr([
                            html.Td("부채비율", style={'font-weight': 'bold', 'text-align': 'center', 'padding': '8px'}),
                            *[html.Td(capital_row[i], style={'text-align': 'center', 'padding': '8px', 'font-weight' : 'bold'}) for i in range(len(capital_row))]
                        ]),                       
                        html.Tr([
                            html.Td("영업이익률", style={'font-weight': 'bold', 'text-align': 'center', 'padding': '8px'}),
                            *[html.Td(profit_margin_row[i], style={'text-align': 'center', 'padding': '8px', 'font-weight' : 'bold'}) for i in range(len(profit_margin_row))]
                        ]), 
                        # Add current stock price and market cap rows
                        html.Tr([
                            html.Td(f"{today_date} 주가", style={'font-weight': 'bold', 'text-align': 'center', 'padding': '8Px'}),
                            html.Td(f"{ind_stock_price} 원", colSpan=len(df.columns) - 1, style={'text-align': 'left', 'padding': '8px'})
                        ]),
                        html.Tr([
                            html.Td("시가총액", style={'font-weight': 'bold', 'text-align': 'center', 'padding': '10px'}),
                            html.Td(ind_formatted_market_price, colSpan=len(df.columns) - 1, style={'text-align': 'left', 'padding': '8px'})
                        ])
                        
                    ])
                ], style={'width': '100%', 'border-collapse': 'collapse', 'border': '1px solid black', 'text-align': 'center', 'font-size': '11px'}),
            ], style={'overflow-x': 'auto', 'width': '100%', 'display': 'block'}),  # Scrollable container styling
            html.Div("* 단 순차입금은 총차입금에서 현금성자산만 제외한 것", style={'color': 'gray', 'font-size': '11px', 'margin-top': '5px', 'text-align': 'right'})
        ])
        if n_clicks and n_clicks > 0:
            csv_data = df.copy()

            # Add 영업이익률, 현재 주가, and 시가총액 rows for CSV output
            csv_debt_row = pd.DataFrame([['순차입금'] + debt_row.tolist()], columns = df.columns)
            csv_capital_row = pd.DataFrame([['부채비율'] + capital_row.tolist()], columns=df.columns)
            csv_profit_margin_row = pd.DataFrame([['영업이익률'] + profit_margin_row.tolist()], columns=df.columns)  # '영업이익률' 추가 및 리스트 변환
            csv_stock_market_rows = pd.DataFrame({'항목명': [f"{today_date} 주가", '시가총액'], 
                                                '19.4Q': [ind_stock_price, ind_formatted_market_price]})

            # Concatenate all rows
            csv_data = pd.concat([csv_data, csv_capital_row, csv_debt_row, csv_profit_margin_row, csv_stock_market_rows], ignore_index=True)

            # Generate CSV string
            csv_string = csv_data.to_csv(index=False, encoding='utf-8-sig')
            return table_output, dict(content=csv_string, filename=f"2019 ~ 2024 {selected_company}_시계열 (단위 : 억원).csv")
        n_clicks = 0
        return table_output, None

    except Exception as e:
        return f"오류 발생: {str(e)}", None


@app.callback(
    Output('independent-rate-change-results', 'children'),
    [Input('independent-year-dropdown', 'value'),
     Input('independent-rate-change-slider', 'value'),
     Input('independent-financial-statement-dropdown', 'value'),
     Input('independent-report-dropdown', 'value'),
     Input('independent-statement-type-dropdown', 'value'),
     Input('independent-compare-report-dropdown', 'value'),  # 비교할 보고서 선택 (Optional)
     Input('independent-item-dropdown', 'value')]  # 항목명 선택 추가
)
def independent_rate_change_results(selected_year, selected_rate_change, selected_financial_statement, selected_report, selected_statement_type, compare_report, selected_items):
    print(f"선택된 연도: {selected_year}, 선택된 등락 비율: {selected_rate_change}, 선택된 재무제표: {selected_financial_statement}, 선택된 보고서: {selected_report}, 비교 보고서: {compare_report}, 선택된 재무제표명: {selected_statement_type}, 선택된 항목: {selected_items}")
    
    # 입력값 검증
    if not selected_year or not selected_rate_change or not selected_financial_statement or not selected_report or not selected_statement_type:
        return "연도, 등락 비율, 재무제표종류, 보고서, 재무제표명을 선택하세요."
    
    selected_info = html.Div([
        html.P(f"선택된 연도: {selected_year}"),
        html.P(f"선택된 등락 비율: {selected_rate_change}%"),
        html.P(f"선택된 재무제표: {selected_financial_statement}"),
        html.P(f"선택된 보고서: {selected_report}"),
        html.P(f"비교할 보고서: {compare_report if compare_report else '비교 없음'}"),
        html.P(f"선택된 재무제표명: {selected_statement_type}"),
        html.P(f"선택된 항목: {', '.join(selected_items) if selected_items else '전체 항목'}")
    ], style={'text-align': 'center'})

    underline_accounts_income = ["매출액", "영업이익", "당기순이익", '지배주주순이익']
    underline_accounts_financial = ["자산총계", '부채총계', "자본총계"]

    if selected_financial_statement == '재무상태표':
        data = financial_data  # 재무상태표 데이터
        keywords = underline_accounts_financial
    elif selected_financial_statement == '손익계산서':
        data = income_statement_data  # 손익계산서 데이터
        keywords = underline_accounts_income
    else:
        data = cashflow_data  # 현금흐름표 데이터
        keywords = []

    # 선택된 항목이 있는 경우 keywords를 필터링
    if selected_items:
        keywords = [item for item in keywords if item in selected_items]

    # 보고서 순서 설정 (1분기 -> 반기 -> 3분기 -> 사업보고서)
    report_order = ['1분기보고서', '반기보고서', '3분기보고서', '사업보고서']

    # 보고서 순서에 따라 최신 보고서가 더 나중에 나와야 하므로 비교 순서를 조정
    if compare_report and report_order.index(compare_report) > report_order.index(selected_report):
        current_column = get_columns_by_report_type(compare_report, selected_financial_statement)
        previous_column = get_columns_by_report_type(selected_report, selected_financial_statement)
        filtered_data_previous_year = data[(data['보고서종류'] == selected_report) & 
                                           (data['재무제표명'] == selected_statement_type) & 
                                           (data['결산연도'] == selected_year) & 
                                           (data['항목명'].isin(keywords))]
        filtered_data_current_year = data[(data['보고서종류'] == compare_report) & 
                                          (data['재무제표명'] == selected_statement_type) & 
                                          (data['결산연도'] == selected_year) & 
                                          (data['항목명'].isin(keywords))]
    else:
        current_column = get_columns_by_report_type(selected_report, selected_financial_statement)
        previous_column = get_columns_by_report_type(compare_report, selected_financial_statement) if compare_report else current_column
        filtered_data_previous_year = data[(data['보고서종류'] == (compare_report if compare_report else selected_report)) & 
                                           (data['재무제표명'] == selected_statement_type) & 
                                           (data['결산연도'] == (selected_year - 1)) & 
                                           (data['항목명'].isin(keywords))]
        filtered_data_current_year = data[(data['보고서종류'] == selected_report) & 
                                          (data['재무제표명'] == selected_statement_type) & 
                                          (data['결산연도'] == selected_year) & 
                                          (data['항목명'].isin(keywords))]

    if filtered_data_previous_year.empty or filtered_data_current_year.empty:
        return f"{selected_year} 또는 {selected_year-1}에 대한 데이터가 없습니다. 다른 연도를 선택하세요."

    # 등락 비율 구간 설정
    lower_bound = selected_rate_change  # 슬라이더 값 기준 이상의 데이터만 필터링
    print(f"등락 비율 기준: {lower_bound}% 이상")

    result_list = []
    for company in filtered_data_current_year['회사명'].unique():
        current_year_data = filtered_data_current_year[filtered_data_current_year['회사명'] == company]
        previous_year_data = filtered_data_previous_year[filtered_data_previous_year['회사명'] == company]

        if not previous_year_data.empty and not current_year_data.empty:
            for 항목명 in keywords:
                previous_value = previous_year_data[previous_year_data['항목명'] == 항목명][previous_column].values[0] if not previous_year_data[previous_year_data['항목명'] == 항목명].empty else np.nan
                current_value = current_year_data[current_year_data['항목명'] == 항목명][current_column].values[0] if not current_year_data[current_year_data['항목명'] == 항목명].empty else np.nan

                if pd.notna(previous_value) and pd.notna(current_value):
                    try:
                        rate_change = calculate_change(float(current_value), float(previous_value))
                        
                        # 주어진 lower_bound와 upper_bound 범위 내의 항목만 포함
                        if abs(rate_change) >= lower_bound:
                            formatted_change = f"{rate_change:.2f}% ▼" if rate_change < 0 else f"{rate_change:.2f}% ▲"
                            color = 'blue' if rate_change < 0 else 'red'  # 색상 반전
                            result_list.append({
                                '회사명': company,
                                '항목명': 항목명,
                                f'{selected_report}' if compare_report else f'{selected_year-1} 연도': f"{previous_value:,.2f}",
                                f'{compare_report}' if compare_report else f'{selected_year} 연도': f"{current_value:,.2f}",
                                '변화율': formatted_change,
                                'sort_value': rate_change,  # 정렬을 위한 숫자값 추가
                                'color': color
                            })
                    except ValueError as e:
                        print(f"변화율 계산 오류: {e}")
                        continue

    if result_list:
        df = pd.DataFrame(result_list)
        # 숫자값으로 정렬 후 sort_value 컬럼 제거
        df = df.sort_values(by=['sort_value'], ascending=False).drop(columns=['sort_value'])

        # 테이블 생성
        result_table = dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[
                {"name": "회사명", "id": "회사명"},
                {"name": "항목명", "id": "항목명"},
                {"name": f'{selected_report}' if compare_report else f'{selected_year-1} 연도', "id": f'{selected_report}' if compare_report else f'{selected_year-1} 연도'},
                {"name": f'{compare_report}' if compare_report else f'{selected_year} 연도', "id": f'{compare_report}' if compare_report else f'{selected_year} 연도'},
                {"name": "변화율", "id": "변화율"}
            ],
            sort_action='native',
            style_cell={'textAlign': 'left', 'padding': '5px'},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{color} = blue', 'column_id': '변화율'},
                    'color': 'blue',
                    'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': '{color} = red', 'column_id': '변화율'},
                    'color': 'red',
                    'fontWeight': 'bold'
                }
            ],
            style_header={
                'backgroundColor': 'lightgrey',
                'fontWeight': 'bold',
                'border': '1px solid black'
            },
            style_data={
                'border': '1px solid grey'
            },
            page_size=25
        )

        return html.Div([selected_info, result_table])
    else:
        return html.Div([selected_info, "선택한 구간에 해당하는 회사가 없습니다."])

app.clientside_callback(
    """
    function(n_clicks) {
        if (n_clicks) {
            const element = document.getElementById('table-and-info');
            if (element) {
                html2canvas(element).then(canvas => {
                    const link = document.createElement('a');
                    link.href = canvas.toDataURL();
                    link.download = 'financial_table.png';
                    link.click();
                }).catch(function(error) {
                    console.error('Error capturing the table: ', error);
                });
            } else {
                console.warn('Table element not found.');
            }
        }
        return null;
    }
    """,
    Output('capture-div', 'children'),
    Input('download-btn', 'n_clicks')
)

# 앱 실행
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)