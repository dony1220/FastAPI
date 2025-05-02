from fastapi import FastAPI, Query, HTTPException, status, Depends
from typing import List, Optional, Union
from fastapi.responses import JSONResponse, HTMLResponse    
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import List
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
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8"
}


# FastAPI 인스턴스 생성
app = FastAPI()
security = HTTPBasic()



VALID_USERNAME = "thesignal"
VALID_PASSWORD = "sporbiz1234"

# 인증 함수 정의
def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != VALID_USERNAME or credentials.password != VALID_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

@app.get("/", response_class=HTMLResponse)
async def home():
    """
    홈페이지 반환
    """
    return templates.TemplateResponse("home.html", {"request": {}})

@app.get("/dash", response_class=HTMLResponse)
async def root(credentials: HTTPBasicCredentials = Depends(authenticate)):
    """
    HTML 페이지 반환 (index.html) - 인증 필요
    """
    return templates.TemplateResponse("index.html", {"request": {}})

@app.get("/list", response_class=HTMLResponse)
async def root(credentials: HTTPBasicCredentials = Depends(authenticate)):
    """
    HTML 페이지 반환 (index.html) - 인증 필요
    """
    return templates.TemplateResponse("list.html", {"request": {}})
# async def root():
#     """
#     HTML 페이지 반환 (index.html)
#     """
#     return templates.TemplateResponse("index.html", {"request": {}})

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
        connection = pymysql.connect(**DB_CONFIG)
        query = """
        SELECT DISTINCT 회사명 
        FROM (
            SELECT 회사명 FROM BalanceSheet
            UNION 
            SELECT 회사명 FROM IncomeStatement
            UNION
            SELECT 회사명 FROM CashFlow
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
        connection = pymysql.connect(**DB_CONFIG)
        query = """
        SELECT DISTINCT 재무제표명 
        FROM (
            SELECT 재무제표명 FROM BalanceSheet
            UNION 
            SELECT 재무제표명 FROM IncomeStatement
            UNION
            SELECT 재무제표명 FROM CashFlow 
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        statement_type_options = [{'label': statement, 'value': statement} for statement in df['재무제표명'].unique()]
        return JSONResponse(content={"statement_type_options": statement_type_options})

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
@app.get("/item-options")
async def get_item_options(
    selected_company: str = Query(...),
    selected_statement_type: str = Query(...),
    selected_report_type: str = Query(...),
    selected_binance_type: str = Query(...)
):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        if selected_binance_type == "손익계산서":
                query = """
                    SELECT DISTINCT 항목명
                    FROM IncomeStatement
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = %s
                    AND 재무제표종류 = %s
                    AND 항목명 IS NOT NULL
                    AND 항목명 != ''
                """
        elif selected_binance_type == "재무상태표":
                query = """
                    SELECT DISTINCT 항목명
                    FROM BalanceSheet
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = %s
                    AND 재무제표종류 = %s
                    AND 항목명 IS NOT NULL
                    AND 항목명 != ''
                """
        elif selected_binance_type == "현금흐름표":
                query = """
                    SELECT DISTINCT 항목명
                    FROM CashFlow
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = %s
                    AND 재무제표종류 = %s
                    AND 항목명 IN(
                        '영업활동현금흐름',
                        '투자활동현금흐름',
                        '유상증자',
                        '재무활동현금흐름',
                        '배당금의지급',
                        '자기주식의취득',
                        '현금의증감'
                    )
                """
        else:
            return JSONResponse(content={"error": "유효하지 않은 재무제표종류입니다."}, status_code=400)
        df = pd.read_sql(query, connection, params=(selected_company, selected_statement_type, selected_report_type, selected_binance_type))
        connection.close()

        item_options = [{'label': item, 'value': item} for item in df['항목명'].unique()]
        return JSONResponse(content={"item_options": item_options})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/report-options")
async def get_report_options():
    '''
    보고서종류
    '''
    try:
        connection = pymysql.connect(**DB_CONFIG)
        query = """
        SELECT DISTINCT 보고서종류 
        FROM (
            SELECT 보고서종류 FROM BalanceSheet
            UNION 
            SELECT 보고서종류 FROM IncomeStatement
            UNION
            SELECT 보고서종류 FROM CashFlow
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        report_options = [{'label': report, 'value': report} for report in df['보고서종류'].unique()]
        return JSONResponse(content={"report_options": report_options})
    
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/binance-options")
async def get_binance_options():
    '''
    재무제표종류(손익, 재무, 손익)
    '''
    try:
        connection = pymysql.connect(**DB_CONFIG)
        query = """
        SELECT DISTINCT 재무제표종류
        FROM (
            SELECT 재무제표종류 FROM BalanceSheet
            UNION 
            SELECT 재무제표종류 FROM IncomeStatement
            UNION
            SELECT 재무제표종류 FROM CashFlow
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        binance_options = [{'label': binance, 'value': binance} for binance in df['재무제표종류'].unique()]
        return JSONResponse(content={"binance_options": binance_options})
    
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)



@app.get("/financial-data")
async def get_financial_data(
    selected_company: str = Query(..., description="선택한 회사명"),
    selected_statement_type: str = Query(..., description="선택한 재무제표명"),
    selected_report_type: str = Query(..., description='선택한 보고서'),
    aggregation: str = Query("quarterly", description="'quarterly' 또는 'cumulative' 중 선택"),
    binance1: Optional[str] = Query(None, description='재무제표종류1'),
    item1: List[str] = Query([], description='항목명1'),
    binance2: Optional[str] = Query(None, description='재무제표종류2'),
    item2: List[str] = Query([], description='항목명2'),
    binance3: Optional[str] = Query(None, description='재무제표종류3'),
    item3: List[str] = Query([], description='항목명3'),
):
    
    report_mapping = {
        "1분기보고서": [3, "1Q"],
        "반기보고서": [6, "2Q"],
        "3분기보고서": [9, "3Q"],
        "사업보고서": [12, "4Q"]
    }

    if selected_report_type not in report_mapping:
        return JSONResponse(content={"error": "유효하지 않은 보고서 종류입니다."}, status_code=400)

    closing_month, quarter_suffix = report_mapping[selected_report_type]

    def get_column_expression(binance_type: str, report_type: str, is_cumulative: bool, aggregation: str) -> str:
        if binance_type == "손익계산서":
            if is_cumulative:
                return {
                    "반기보고서": "당기_반기_누적",
                    "3분기보고서": "당기_3분기_누적"
                }.get(report_type, "당기")
            else:
                if report_type == "사업보고서":
                    return "(당기 - 당기_3분기_누적)"
                return {
                    "1분기보고서": "CASE WHEN 당기_1분기_3개월 IS NOT NULL THEN 당기_1분기_3개월 ELSE 당기_1분기말 END",
                    "반기보고서": "CASE WHEN 당기_반기_3개월 IS NOT NULL THEN 당기_반기_3개월 ELSE 당기_반기말 END",
                    "3분기보고서": "CASE WHEN 당기_3분기_3개월 IS NOT NULL THEN 당기_3분기_3개월 ELSE 당기_3분기말 END",
                    "사업보고서": "당기"
                }.get(report_type, "당기")

        elif binance_type == "재무상태표":
            return {
                "1분기보고서": "당기_1분기말",
                "반기보고서": "당기_반기말",
                "3분기보고서": "당기_3분기말",
                "사업보고서": "당기"
            }.get(report_type, "당기")

        elif binance_type == "현금흐름표":
            return {
                "1분기보고서": "CASE WHEN 당기_1분기 IS NOT NULL THEN 당기_1분기 ELSE 당기_1분기말 END",
                "반기보고서": "CASE WHEN 당기_반기 IS NOT NULL THEN 당기_반기 ELSE 당기_반기말 END",
                "3분기보고서": "CASE WHEN 당기_3분기 IS NOT NULL THEN 당기_3분기 ELSE 당기_3분기말 END",
                "사업보고서": "당기"
            }.get(report_type, "당기")
        else:
            raise ValueError("Invalid binance_type")

    year_columns_template = lambda col_expr: ",\n".join([
        f"FORMAT(MAX(CASE WHEN 결산기준일 = {year} AND 결산월 = {closing_month} THEN {col_expr} END), 0) AS '{str(year)[2:]}.{quarter_suffix}'"
        for year in range(2019, 2025)
    ])

    table_mapping = {
        "손익계산서": "IncomeStatement",
        "재무상태표": "BalanceSheet",
        "현금흐름표": "CashFlow"
    }

    binance_item_pairs = [
        (binance1, item1),
        (binance2, item2),
        (binance3, item3)
    ]    
    valid_pairs = [(b, i) for b, items in binance_item_pairs for i in items if b and i]
    if not valid_pairs:
        return JSONResponse(content={"error": "최소 1개의 재무제표종류와 항목명을 선택해야 합니다."}, status_code=400)

    stock_code_sql = """
        SELECT DISTINCT CAST(종목코드 AS CHAR) AS 종목코드
        FROM IncomeStatement
        WHERE 회사명 = %s;
    """

    try:
        connection = pymysql.connect(**DB_CONFIG)
        stock_code_df = pd.read_sql(stock_code_sql, connection, params=(selected_company))
        if stock_code_df.empty:
            return JSONResponse(content={"error": "종목코드를 찾을 수 없습니다."}, status_code=404)
        stock_code = stock_code_df["종목코드"].iloc[0]

        dfs = []
        for binance, item in valid_pairs:
            table_name = table_mapping.get(binance)
            if not table_name:
                continue

            if (selected_report_type == "사업보고서" and aggregation == "quarterly" and binance == "손익계산서"):
        # 사업보고서 '당기' 조회
                biz_sql = f"""
                    SELECT 항목명,
                    {", ".join([
                        f"FORMAT(MAX(CASE WHEN 결산기준일 = {year} AND 결산월 = 12 THEN 당기 END), 0) AS '{str(year)[2:]}.4Q'"
                        for year in range(2019, 2025)
                    ])}
                    FROM {table_name}
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = '사업보고서'
                    AND 재무제표종류 = %s
                    AND 항목명 = %s
                    GROUP BY 항목명
                """
                biz_df = pd.read_sql(biz_sql, connection, params=(selected_company, selected_statement_type, binance, item))

                # 3분기보고서 '당기_3분기_누적' 조회
                q3_sql = f"""
                    SELECT 항목명,
                    {", ".join([
                        f"FORMAT(MAX(CASE WHEN 결산기준일 = {year} AND 결산월 = 9 THEN 당기_3분기_누적 END), 0) AS '{str(year)[2:]}.3Q'"
                        for year in range(2019, 2025)
                    ])}
                    FROM {table_name}
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = '3분기보고서'
                    AND 재무제표종류 = %s
                    AND 항목명 = %s
                    GROUP BY 항목명
                """
                q3_df = pd.read_sql(q3_sql, connection, params=(selected_company, selected_statement_type, binance, item))

                if biz_df.empty:
                    continue

                if q3_df.empty:
                    # 3분기 데이터가 없으면 그냥 사업보고서 당기 데이터 사용
                    dfs.append(biz_df)
                else:
                    # 두 df를 항목명으로 merge
                    merged = pd.merge(biz_df, q3_df, on="항목명", how="left")

                    # 계산할 연도 리스트
                    result_rows = []
                    for idx, row in merged.iterrows():
                        result = {"항목명": row["항목명"]}
                        for year in range(2019, 2025):
                            year_4q = f"{str(year)[2:]}.4Q"
                            year_3q = f"{str(year)[2:]}.3Q"
                            
                            # 둘 다 값이 있으면 계산
                            try:
                                value_4q = float(str(row[year_4q]).replace(",", "")) if pd.notna(row[year_4q]) else None
                                value_3q = float(str(row[year_3q]).replace(",", "")) if pd.notna(row[year_3q]) else 0  # 없으면 0으로
                                if value_4q is not None:
                                    result[year_4q] = "{:,.0f}".format(value_4q - value_3q)
                                else:
                                    result[year_4q] = None
                            except Exception as e:
                                result[year_4q] = None
                        result_rows.append(result)

                    result_df = pd.DataFrame(result_rows)
                    dfs.append(result_df)
            else:
                #이부분 새로 수정 사업보고서 분기 실적
                column_expr = get_column_expression(binance, selected_report_type, aggregation == "cumulative", aggregation)
                year_columns = year_columns_template(column_expr)
                sql = f"""
                    SELECT 항목명,\n{year_columns}
                    FROM {table_name}
                    WHERE 회사명 = %s
                    AND 재무제표명 = %s
                    AND 보고서종류 = %s
                    AND 재무제표종류 = %s
                    AND 항목명 = %s
                    GROUP BY 항목명
                """
                df = pd.read_sql(sql, connection, params=(selected_company, selected_statement_type, selected_report_type, binance, item))
                if not df.empty:
                    dfs.append(df)

        connection.close()
        if not dfs:
            return JSONResponse(content={"message": "데이터가 없습니다."}, status_code=404)

        merged_df = pd.concat(dfs, ignore_index=True)



        # 데이터 처리 및 시각화 준비
        # if df.empty:
        #     return JSONResponse(content={"message": "데이터가 없습니다."}, status_code=404)
        
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
                    return f"(▲ {change:.1f}%)", 'red'  # 상승
                elif change < 0:
                    return f"(▼ {change:.1f}%)", 'blue'  # 하락
            return '-', 'black'  # 변화 없음
        
        
        def reclean_and_reconvert(series):
            return series.str.replace(",", "").str.replace("%", "").astype(float)


        # 쉼표를 다시 추가하는 함수

        
        def get_stock_price(stock_code):
            try:
                # stock_today = datetime.datetime.today().strftime('%Y-%m-%d')
                stock_yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                stock_data = fdr.DataReader(stock_code, stock_yesterday, stock_yesterday)
                if stock_data.empty:
                    return "주가 데이터 없음 (휴일등으로 데이터가 없을 경우 직접확인 요망)"
                return stock_data['Close'].values[0]
            except Exception as e:
                print(f"주가 데이터 없음 (휴일등으로 데이터가 없을 경우 직접확인 요망)")
                return f"주가 데이터 없음"

        def get_market_cap(stock_code):
            try:
                marketcap_yesterday = (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
                # today = datetime.datetime.today().strftime('%Y%m%d')
                stock_data = stock.get_market_cap(marketcap_yesterday, marketcap_yesterday, stock_code)
                if stock_data.empty:
                    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                    stock_data = stock.get_market_cap(yesterday, yesterday, stock_code)
                if stock_data.empty:
                    return "시총 데이터 없음 (휴일등으로 데이터가 없을 경우 직접확인 요망)"
                return stock_data["시가총액"].values[0]
            except Exception as e:
                print(f"시총 데이터 없음 (휴일등으로 데이터가 없을 경우 직접확인 요망)")
                return f"시가총액 조회 오류"
        
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
        # today_today = datetime.datetime.today().strftime('%m월 %d일')

        ref_col = merged_df.columns[1]  # 첫 번째 분기 열 기준

        
        stock_price_row = {
            "항목명": '전일종가',
            ref_col: f'{stock_price:,}원' if isinstance(stock_price, (int, float)) else stock_price,
            **{col: None for col in merged_df.columns if col not in ["항목명", ref_col]}
        }
        market_cap_row = {
            "항목명": "전일 시가총액",
            ref_col: formatted_market_cap,
            **{col: None for col in merged_df.columns if col not in ["항목명", ref_col]}
        }
        merged_df = pd.concat([merged_df, pd.DataFrame([stock_price_row, market_cap_row])], ignore_index=True)
        
        quarter_columns = [col for col in merged_df.columns if col.endswith('Q')]
        quarter_columns.sort(key=lambda x: int(x.split('.')[0]))

        all_selected_items = set((item1 or []) + (item2 or []) + (item3 or []))


        if len(quarter_columns) >= 2:
            for index, row in merged_df.iterrows():
                항목명 = str(row['항목명'])
                if 항목명 in all_selected_items:
                    try:
                        curr_col = quarter_columns[-1]
                        prev_col = quarter_columns[-2]
                        if pd.isna(row[curr_col]) or row[curr_col] == "-":
                            curr_col = prev_col
                            prev_col = quarter_columns[-3] if len(quarter_columns) >= 3 else None

                        if prev_col is None:
                            continue

                        prev_value = reclean_and_reconvert(pd.Series([row[prev_col]])).iloc[0]
                        curr_value = reclean_and_reconvert(pd.Series([row[curr_col]])).iloc[0]

                        change = calculate_change(curr_value, prev_value)
                        formatted_change, color = format_change(change)

                        if formatted_change != '-':
                            merged_df.at[index, curr_col] += f' <span style="color: {color};">{formatted_change}</span>'
                    except Exception as e:
                        print(f"오류 발생 (행 : {index}) : {e}")
                        continue
        # 결과 반환
        return JSONResponse(content={
            "financial_data": merged_df.to_dict(orient="records")})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

#---------------------------------------------------------------재무데이터 ------------------------------------------------------------------------------------------------




@app.get('/list', response_class=HTMLResponse)
async def list():
    return templates.TemplateResponse('list.html', {"request": {}})

def get_columns_by_report_type(report_type, statement_type):
    if statement_type == "손익계산서":
        mapping = {
            "1분기보고서": ["당기_1분기_3개월", "당기_1분기말"],
            "반기보고서": ["당기_반기_3개월", "당기_반기말"],
            "3분기보고서": ["당기_3분기_3개월", "당기_3분기말"],
            "사업보고서": ["당기"]
        }
    elif statement_type == "현금흐름표":
        mapping = {
            "1분기보고서": ["당기_1분기", "당기_1분기말"],
            "반기보고서": ["당기_반기", "당기_반기말"],
            "3분기보고서": ["당기_3분기", "당기_3분기말"],
            "사업보고서": ["당기"]
        }
    else:
        mapping = {
            "1분기보고서": ["당기_1분기말"],
            "반기보고서": ["당기_반기말"],
            "3분기보고서": ["당기_3분기말"],
            "사업보고서": ["당기"]
        }
    return mapping.get(report_type, ["당기"])



def calculate_change(current, previous):
    try:
        if pd.notna(current) and pd.notna(previous) and previous != 0:
            return (current - previous) / abs(previous) * 100
    except (TypeError, ValueError):
        return np.nan
    return np.nan


@app.get("/independent-report-input")
async def get_report_options():
    '''
    보고서종류
    '''
    try:
        connection = pymysql.connect(**DB_CONFIG)
        query = """
        SELECT DISTINCT 보고서종류 
        FROM (
            SELECT 보고서종류 FROM BalanceSheet
            UNION 
            SELECT 보고서종류 FROM IncomeStatement
            UNION
            SELECT 보고서종류 FROM CashFlow
        ) AS combined
        """
        df = pd.read_sql(query, connection)
        connection.close()

        report_options = [{'label': report, 'value': report} for report in df['보고서종류'].unique()]
        return JSONResponse(content={"report_options": report_options})
    
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    


@app.get("/rate-change-analysis")
async def rate_change_analysis(
    selected_year: int = Query(...),
    selected_rate_change: float = Query(...),
    selected_financial_statement: str = Query(...),
    selected_report: str = Query(...),
    compare_report: Optional[str] = Query(None),
    selected_statement_type: str = Query(...),
    selected_items: Union[List[str], str, None] = Query(None)
):
    underline_accounts_income = ["매출액", "영업이익", "당기순이익"]
    underline_accounts_financial = ["자산총계", '부채총계', "자본총계"]
    underline_accounts_cash = ['영업활동현금흐름', '배당금의지급', '투자활동현금흐름', '재무활동현금흐름', '자기주식의취득', '현금의증감', '유상증자']

    table_name = "BalanceSheet" if selected_financial_statement == '재무상태표' else (
        "IncomeStatement" if selected_financial_statement == '손익계산서' else "CashFlow")

    keywords = underline_accounts_financial if selected_financial_statement == '재무상태표' else (
        underline_accounts_income if selected_financial_statement == '손익계산서' else underline_accounts_cash)

    if selected_items:
        keywords = [item for item in keywords if item in selected_items]

    try:
        conn = pymysql.connect(**DB_CONFIG)
        query = f"""
            SELECT * FROM {table_name}
            WHERE 결산기준일 BETWEEN %s AND %s
            AND 재무제표명 = %s
            AND 항목명 IN ({', '.join(['%s'] * len(keywords))})
        """
        params = [selected_year - 1, selected_year, selected_statement_type] + keywords
        df = pd.read_sql(query, conn, params=params)
        conn.close()
    except Exception as e:
        return JSONResponse(content={"error": f"데이터 로딩 오류: {str(e)}"}, status_code=500)

    if df.empty:
        return JSONResponse(content={"message": "선택된 조건에 대한 데이터가 없습니다."}, status_code=404)

    report_order = ['1분기보고서', '반기보고서', '3분기보고서', '사업보고서']
    if compare_report and report_order.index(compare_report) > report_order.index(selected_report):
        current_column = get_columns_by_report_type(compare_report, selected_financial_statement)
        previous_column = get_columns_by_report_type(selected_report, selected_financial_statement)
        prev_data = df[(df['보고서종류'] == selected_report) & (df['결산기준일'] == selected_year) & (df['항목명'].isin(keywords))]
        curr_data = df[(df['보고서종류'] == compare_report) & (df['결산기준일'] == selected_year) & (df['항목명'].isin(keywords))]
        previous_label = selected_report
        current_label = compare_report
    else:
        current_column = get_columns_by_report_type(selected_report, selected_financial_statement)
        previous_column = get_columns_by_report_type(compare_report or selected_report, selected_financial_statement)
        prev_data = df[(df['보고서종류'] == (compare_report or selected_report)) & (df['결산기준일'] == selected_year - 1) & (df['항목명'].isin(keywords))]
        curr_data = df[(df['보고서종류'] == selected_report) & (df['결산기준일'] == selected_year) & (df['항목명'].isin(keywords))]
        previous_label = f"{selected_year - 1}년"
        current_label = f"{selected_year}년"

    if prev_data.empty or curr_data.empty:
        return JSONResponse(content={"message": "해당 연도 또는 비교 연도 데이터가 부족합니다."}, status_code=404)

    results = []
    for company in curr_data['회사명'].unique():
        curr = curr_data[curr_data['회사명'] == company]
        prev = prev_data[prev_data['회사명'] == company]
        if not prev.empty and not curr.empty:
            for 항목명 in keywords:
                prev_val = next((prev[prev['항목명'] == 항목명][col].values[0] for col in previous_column if col in prev.columns and not prev[prev['항목명'] == 항목명][col].empty), np.nan)
                curr_val = next((curr[curr['항목명'] == 항목명][col].values[0] for col in current_column if col in curr.columns and not curr[curr['항목명'] == 항목명][col].empty), np.nan)
                # prev_val = prev[prev['항목명'] == 항목명][previous_column].values[0] if not prev[prev['항목명'] == 항목명].empty else np.nan
                # curr_val = curr[curr['항목명'] == 항목명][current_column].values[0] if not curr[curr['항목명'] == 항목명].empty else np.nan
                if pd.notna(prev_val) and pd.notna(curr_val):
                    rate = calculate_change(float(curr_val), float(prev_val))
                    if abs(rate) >= selected_rate_change:
                        results.append({
                            '회사명': company,
                            '항목명': 항목명,
                            previous_label: f"{prev_val:,.2f}",
                            current_label: f"{curr_val:,.2f}",
                            '변화율': f"{rate:.2f}%",
                            'color': 'blue' if rate < 0 else 'red',
                            'sort_value': rate  # 정렬용 추가
                        })
    results.sort(key=lambda x: (x['항목명'], -x['sort_value']))  # 항목명별, 변화율 내림차순
    for r in results:
        r.pop('sort_value', None)

    if not results:
        return JSONResponse(content={"message": "조건을 만족하는 결과가 없습니다."})

    return JSONResponse(content={"results": results})

    

