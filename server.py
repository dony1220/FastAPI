from fastapi import FastAPI, Query
from typing import List
from fastapi.responses import JSONResponse, HTMLResponse    
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import pymysql
from dotenv import load_dotenv
import os
import datetime
import FinanceDataReader as fdr
from pykrx import stock
import numpy as np
import pandas as pd



# 환경 변수 로드
load_dotenv('profile.env')

# 데이터베이스 연결 정보
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT", 3306))  # 기본 포트 3306
db_name = os.getenv("DB_NAME")

# 환경 변수 확인
if not all([db_user, db_password, db_host, db_name]):
    raise EnvironmentError("필수 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")

# FastAPI 인스턴스 생성
app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def home():
    """
    홈페이지 반환
    """
    return templates.TemplateResponse("home.html", {"request": {}})

@app.get("/dash", response_class=HTMLResponse)
async def root():
    """
    HTML 페이지 반환 (index.html)
    """
    return templates.TemplateResponse("index.html", {"request": {}})

# 템플릿 및 정적 파일 설정
templates_dir = "templates"
static_dir = "static"

if not os.path.exists(templates_dir):
    raise FileNotFoundError(f"템플릿 디렉토리가 없습니다: {templates_dir}")
if not os.path.exists(static_dir):
    raise FileNotFoundError(f"정적 파일 디렉토리가 없습니다: {static_dir}")

templates = Jinja2Templates(directory=templates_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/company-options")
async def get_company_options():
    """
    데이터베이스에서 고유한 회사명 가져오기
    """
    try:
        connection = pymysql.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
            charset='utf8'
        )
        query = """
        SELECT DISTINCT 회사명 
        FROM (
            SELECT 회사명 FROM BalanceSheet
            UNION 
            SELECT 회사명 FROM IncomeStatement
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        company_options = [{'label': company, 'value': company} for company in df['회사명'].unique()]
        return JSONResponse(content={"company_options": company_options})

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/statement-options")
async def get_statement_options():
    """
    데이터베이스에서 고유한 재무제표명 가져오기
    """
    try:
        connection = pymysql.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
            charset='utf8'
        )
        query = """
        SELECT DISTINCT 재무제표명 
        FROM (
            SELECT 재무제표명 FROM BalanceSheet
            UNION 
            SELECT 재무제표명 FROM IncomeStatement
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        statement_type_options = [{'label': statement, 'value': statement} for statement in df['재무제표명'].unique()]
        return JSONResponse(content={"statement_type_options": statement_type_options})

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/report-options")
async def get_report_options():
    '''
    보고서종류
    '''
    try:
        connection = pymysql.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
            charset='utf8'
        )
        query = """
        SELECT DISTINCT 보고서종류 
        FROM (
            SELECT 보고서종류 FROM BalanceSheet
            UNION 
            SELECT 보고서종류 FROM IncomeStatement
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        report_options = [{'label': report, 'value': report} for report in df['보고서종류'].unique()]
        return JSONResponse(content={"report_options": report_options})
    
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)



@app.get("/financial-data")
async def get_financial_data(
    selected_company: str = Query(..., description="선택한 회사명"),
    selected_statement_type: str = Query(..., description="선택한 재무제표명"),
    selected_report_type: str = Query(..., description='선택한 보고서'),
):
    
    
    report_mapping = {
        "1분기보고서": [3, 1],
        "반기보고서": [6, 2],
        "3분기보고서": [9, 3],
        "사업보고서": [12, 4]
    }
    
    # 사용자가 선택한 보고서 종류에 따른 결산월 가져오기
    selected_months = report_mapping.get(selected_report_type)
    if not selected_months:
        return {"error": "유효하지 않은 보고서 종류입니다. 1분기, 반기, 3분기, 사업보고서 중 하나를 선택하세요."}
    
    quarter_suffix = f"{selected_months[1]}Q"
    stock_code_sql = """
        SELECT DISTINCT CAST(종목코드 AS CHAR) AS 종목코드
        FROM IncomeStatement
        WHERE 회사명 = %s;
    """


    """
    사용자가 선택한 회사와 재무제표명을 기반으로 시계열 데이터 반환
    """
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
    WHERE 회사명 = %s
        AND 재무제표명 = '연결재무제표'
        AND (
            (항목명 LIKE '%%리스%%' 
            OR 항목명 LIKE '%%차입금%%' 
            OR 항목명 LIKE '%%금융부채%%' 
            OR 항목명 LIKE '%%사채%%'
            OR 항목명 LIKE '%%차입%%')
            AND 항목명 NOT LIKE '%%비금융부채%%'
        )
    GROUP BY 보고서종류, 결산기준일, 결산월
    )
    SELECT 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '19.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '20.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '21.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '22.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '23.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기_3개월
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기_3개월
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기_3개월
                    END
                END), 0) AS '24.{quarter_suffix}',
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
    WHERE 회사명 = %s
        AND 재무제표명 = %s
        AND 항목명 IN ('매출액', '영업이익', '당기순이익')
    GROUP BY 항목명
    UNION ALL 
    SELECT 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '19.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '20.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '21.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '22.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '23.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = %s THEN 
                    CASE 
                        WHEN 보고서종류 = '사업보고서' THEN 당기
                        WHEN 보고서종류 = '3분기보고서' THEN 당기_3분기말
                        WHEN 보고서종류 = '반기보고서' THEN 당기_반기말
                        WHEN 보고서종류 = '1분기보고서' THEN 당기_1분기말
                    END
                END), 0) AS '24.{quarter_suffix}',
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
    WHERE 회사명 = %s
        AND 재무제표명 = %s
        AND 항목명 IN ('자산총계', '부채총계', '자본총계', '현금및현금성자산')
    GROUP BY 항목명
    UNION ALL
    SELECT '총차입금' AS 항목명,
        FORMAT(MAX(CASE WHEN 결산기준일 = 2019 AND 결산월 = %s THEN 총차입금 END), 0) AS '19.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2020 AND 결산월 = %s THEN 총차입금 END), 0) AS '20.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2021 AND 결산월 = %s THEN 총차입금 END), 0) AS '21.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2022 AND 결산월 = %s THEN 총차입금 END), 0) AS '22.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = %s THEN 총차입금 END), 0) AS '23.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = %s THEN 총차입금 END), 0) AS '24.{quarter_suffix}',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2023 AND 결산월 = 9 THEN 총차입금 END), 0) AS '23.3Q 누적',
        FORMAT(MAX(CASE WHEN 결산기준일 = 2024 AND 결산월 = 9 THEN 총차입금 END), 0) AS '24.3Q 누적'
    FROM 차입금_합계
    ORDER BY FIELD(항목명, '매출액', '영업이익', '당기순이익', '자산총계', '부채총계', '자본총계', '총차입금', '현금및현금성자산');
    """


    # 드롭다운 하나 추가해서, 본래 시계열 데이터 하던거 추가하기

    try:
        connection = pymysql.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
            charset='utf8'
        )
        stock_code_df = pd.read_sql(stock_code_sql, connection, params=(selected_company))
        if stock_code_df.empty:
            return JSONResponse(content={"error": "종목코드를 찾을 수 없습니다."}, status_code=404)
        stock_code = stock_code_df["종목코드"].iloc[0]

        df = pd.read_sql(sql, connection, params=(selected_company, selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_company, selected_statement_type, selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_company, selected_statement_type,
                                                  selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0], selected_months[0]))
        connection.close()



        # 데이터 처리 및 시각화 준비
        if df.empty:
            return JSONResponse(content={"message": "데이터가 없습니다."}, status_code=404)
        
        def calculate_change(current, previous):
            try:
                if pd.notna(current) and pd.notna(previous) and previous != 0:
                    return (current - previous) / abs(previous) * 100
            except (TypeError, ValueError):
                return np.nan
            return np.nan

        # 변화율 포맷팅 함수
        def format_change(change):
            if pd.notna(change):
                if change > 0:
                    return f"({change:.1f}%) ▲", 'red'  # 상승
                elif change < 0:
                    return f"({change:.1f}%) ▼", 'blue'  # 하락
            return '-', 'black'  # 변화 없음
        
        def clean_and_convert(series):
            return series.str.replace(",", "").astype(float)
        
        def reclean_and_reconvert(series):
            return series.str.replace(",", "").str.replace("%", "").astype(float)


        # 쉼표를 다시 추가하는 함수
        def add_commas(value):
            return f"{value:,}" if isinstance(value, (float, int)) and not pd.isna(value) else "-"
        
        def get_stock_price(stock_code):
            try:
                today = datetime.datetime.today().strftime('%Y-%m-%d')
                stock_data = fdr.DataReader(stock_code, today, today)
                if stock_data.empty:
                    return "주가 데이터 없음"
                return stock_data['Close'].values[0]
            except Exception as e:
                print(f"주가 데이터 없음: {str(e)}")
                return f"주가 조회 오류: {str(e)}"

        def get_market_cap(stock_code):
            try:
                today = datetime.datetime.today().strftime('%Y%m%d')
                stock_data = stock.get_market_cap(today, today, stock_code)
                if stock_data.empty:
                    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                    stock_data = stock.get_market_cap(yesterday, yesterday, stock_code)
                if stock_data.empty:
                    return "시총 데이터 없음"
                return stock_data["시가총액"].values[0]
            except Exception as e:
                print(f"시총 데이터 없음: {str(e)}")
                return f"시가총액 조회 오류: {str(e)}"
        
        def format_market_price(market_price):
            try:
                if isinstance(market_price, str):
                    return market_price  # 오류 메시지 반환
                if market_price >= 1_0000_0000_0000:  # 1조 이상
                    조 = market_price // 1_0000_0000_0000
                    억 = (market_price % 1_0000_0000_0000) // 1_0000_0000
                    return f"{조}조 {억}억" if 억 > 0 else f"{조}조"
                elif market_price >= 1_0000_0000:  # 1억 이상
                    억 = market_price // 1_0000_0000
                    return f"{억}억"
                else:
                    return f"{market_price}"
            except (TypeError, ValueError):
                return "데이터 불러오기 실패"
            
        stock_price = get_stock_price(stock_code)
        market_cap = get_market_cap(stock_code)  
        formatted_market_cap = format_market_price(market_cap)

        
        if '총차입금' in df['항목명'].values and '현금및현금성자산' in df['항목명'].values:
            try:
                debt_row = clean_and_convert(df[df['항목명'] == '총차입금'].iloc[0, 1:])
                cash_row = clean_and_convert(df[df['항목명'] == '현금및현금성자산'].iloc[0, 1:])
                pury_row = []
                for debt,cash in zip(debt_row, cash_row):
                    try:
                        pury = (debt - cash) if pd.notna(debt) and pd.notna(cash) else None
                        pury_row.append(int(pury) if pury is not None else '-')
                    except Exception:
                        pury_row.append("-")
                pury_row_row = {"항목명" : "순차입금"}
                for col,value in zip(df.columns[1:], pury_row):
                    pury_row_row[col] = f"{add_commas(value)}" if value != '-' else '-'
                df = pd.concat([df, pd.DataFrame([pury_row_row])], ignore_index=True)
            except Exception as e:
                return JSONResponse(content={'error' : f"순차입금 계산 중 오류 발생: {str(e)}"}, status_code=500)
        # 부채비율 계산
        if '부채총계' in df['항목명'].values and '자본총계' in df['항목명'].values:
            try:
                debt_row = clean_and_convert(df[df['항목명'] == '부채총계'].iloc[0, 1:])
                equity_row = clean_and_convert(df[df['항목명'] == '자본총계'].iloc[0, 1:])
                liability_ratio = []

                for debt, equity in zip(debt_row, equity_row):
                    try:
                        ratio = (debt / equity * 100) if pd.notna(debt) and pd.notna(equity) else None
                        liability_ratio.append(int(ratio) if ratio is not None else '-')
                    except Exception:
                        liability_ratio.append("-")  # 계산 중 오류 발생 시 "-" 처리

                liability_ratio_row = {"항목명": "부채비율"}
                for col, value in zip(df.columns[1:], liability_ratio):
                    liability_ratio_row[col] = f"{add_commas(value)}%" if value != "-" else "-"

                df = pd.concat([df, pd.DataFrame([liability_ratio_row])], ignore_index=True)

            except Exception as e:
                return JSONResponse(content={"error": f"부채비율 계산 중 오류 발생: {str(e)}"}, status_code=500)

        # 영업이익률 계산
        if '매출액' in df['항목명'].values and '영업이익' in df['항목명'].values:
            try:
                sales_row = clean_and_convert(df[df['항목명'] == '매출액'].iloc[0, 1:])
                operating_income_row = clean_and_convert(df[df['항목명'] == '영업이익'].iloc[0, 1:])
                profit_margin = []

                for sales, income in zip(sales_row, operating_income_row):
                    try:
                        margin = (income / sales * 100) if pd.notna(sales) and pd.notna(income) else None
                        profit_margin.append(int(margin) if margin is not None else '-')
                    except Exception:
                        profit_margin.append("-")  # 계산 중 오류 발생 시 "-" 처리

                profit_margin_row = {"항목명": "영업이익률"}
                for col, value in zip(df.columns[1:], profit_margin):
                    profit_margin_row[col] = f"{add_commas(value)}%" if value != "-" else "-"

                df = pd.concat([df, pd.DataFrame([profit_margin_row])], ignore_index=True)

            except Exception as e:
                return JSONResponse(content={"error": f"영업이익률 계산 중 오류 발생: {str(e)}"}, status_code=500)
            
        today_today = datetime.datetime.today().strftime('%m월 %d일')
        stock_price_row = {
            "항목명": f'{today_today} 주가',
            df.columns[1]: f'{stock_price:,}원' if isinstance(stock_price, (int, float)) else f"{stock_price}원",
            **{col: None for col in df.columns[2:]},
        }
        market_cap_row = {
            "항목명": "시가총액",
            df.columns[1]: formatted_market_cap if isinstance(market_cap, (int, float)) else formatted_market_cap,
            **{col: None for col in df.columns[2:]},
        }
        df = pd.concat([df, pd.DataFrame([stock_price_row, market_cap_row])], ignore_index=True)
        
        quarter_columns = [col for col in df.columns if col.endswith('Q')]  # '3Q', '4Q' 등으로 끝나는 열 찾기
        quarter_columns.sort(key=lambda x: int(x.split('.')[0]))  # 연도 기준 정렬

        if len(quarter_columns) >= 2:  # 최소 두 개의 분기가 있어야 비교 가능
            for index, row in df.iterrows():
                try:
                    # 최신 분기와 이전 분기를 초기화
                    curr_col = quarter_columns[-1]  # 최신 분기
                    prev_col = quarter_columns[-2]  # 이전 분기

                    # 최신 분기 데이터가 없는 경우, 이전 분기와 그 이전 분기를 비교
                    if pd.isna(row[curr_col]) or row[curr_col] == "-":
                        curr_col = prev_col  # 최신 분기를 이전 분기로 대체
                        prev_col = quarter_columns[-3] if len(quarter_columns) >= 3 else None  # 그 이전 분기를 가져옴

                    # 이전 분기가 없는 경우 처리 건너뛰기
                    if prev_col is None:
                        continue

                    # 데이터 추출
                    prev_value = reclean_and_reconvert(pd.Series([row[prev_col]])).iloc[0]
                    curr_value = reclean_and_reconvert(pd.Series([row[curr_col]])).iloc[0]

                    # 변화율 계산
                    change = calculate_change(curr_value, prev_value)
                    formatted_change, color = format_change(change)

                    # 변화율 결과를 추가
                    if formatted_change != '-':
                        df.at[index, curr_col] += f' <span style="color: {color};">{formatted_change}</span>'  # HTML 태그로 색상 적용
                except Exception as e:
                    print(f"오류 발생 (행 : {index}) : {e}")
                    continue
        # 결과 반환
        return JSONResponse(content={
            "financial_data": df.to_dict(orient="records")})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)