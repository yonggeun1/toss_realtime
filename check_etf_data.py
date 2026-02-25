from toss_crawling.supabase_client import supabase
import pandas as pd

def check_tables():
    try:
        res_etf = supabase.table("naver_realtime_etf").select("*", count="exact").limit(5).execute()
        print("--- naver_realtime_etf (Total: " + str(res_etf.count) + ") ---")
        print(res_etf.data)
        
        res_view = supabase.table("naver_realtime_etf_ranking_table").select("*", count="exact").limit(5).execute()
        print("\n--- naver_realtime_etf_ranking_table (Total: " + str(res_view.count) + ") ---")
        print(res_view.data)
        
        res_pdf = supabase.table("ETF_PDF").select("etf_code", count="exact").limit(1).execute()
        print("\n--- ETF_PDF Count: " + str(res_pdf.count) + " ---")

        res_stk = supabase.table("naver_realtime_stk").select("stk_cd", count="exact").limit(1).execute()
        print("--- naver_realtime_stk Count: " + str(res_stk.count) + " ---")
        
    except Exception as e:
        print("Error: " + str(e))

if __name__ == '__main__':
    check_tables()
