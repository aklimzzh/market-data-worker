import akshare as ak

def fetch_snapshot():
    industry=ak.stock_fund_flow_industry_ths(symbol="即时").copy()
    industry["sector_type"]="industry"
    concept=ak.stock_fund_flow_concept_ths(symbol="即时").copy()
    concept["sector_type"]="concept"
    return industry, concept
