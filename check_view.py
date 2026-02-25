from toss_crawling.supabase_client import supabase

def check_data_and_permissions():
    try:
        res_table = supabase.table("naver_realtime_etf").select("*").limit(3).execute()
        print("Table Data:")
        print(res_table.data)
        res_view = supabase.table("naver_realtime_etf_ranking_table").select("*").limit(3).execute()
        print("View Data:")
        print(res_view.data)
    except Exception as e:
        print("Error: " + str(e))

if __name__ == '__main__':
    check_data_and_permissions()
