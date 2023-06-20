import requests
import time
import os
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# ChromeWebdriver自動更新用
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

import env_supabase.supabase_env as sb
from env_person.person import person
import env


statement_dir = rf"{os.path.dirname(__file__)}\card_statement"
statement_filename = f"rakuten-card{datetime.now().strftime('%Y%m')}.csv"
statement_addr = rf"{statement_dir}\{statement_filename}"

# 家賃支払日
rent_use_date  = (datetime.now() + relativedelta(months=1)).strftime("%Y/%m/01")

def main():
    # 当月のデータが登録されているか確認して、データがなければカード明細取得、登録
    if not exists_cur_month_data():
        if not os.path.isfile(statement_addr):
            download_statement()
        statememt_df = get_statement()
        insert_pay_history(statememt_df)

    payment_amount = get_monthly_payment_amount()
    payment_amount_text = f"{person[0]['kanji']}:{int(payment_amount[0]):,}円\n"\
                          f"{person[1]['kanji']}:{int(payment_amount[1]):,}円"

    expenses_each_category = get_monthly_expenses_each_category()
    expenses_each_category_text = "＜詳細＞\n"
    for i in expenses_each_category:
        category = i[0] if i[0] else "未設定"
        expenses_each_category_text += f" {category}: {int(i[1]):,}円"
        if i != expenses_each_category[-1]:
            expenses_each_category_text += "\n"

    msg_to_send = datetime.now().strftime('%Y/%m')
    msg_to_send += "\n"
    msg_to_send += payment_amount_text
    msg_to_send += "\n"
    msg_to_send += expenses_each_category_text
    print(msg_to_send)
    send_msg(msg_to_send,
            #  is_test=True
            )

# カード明細をDLする
def download_statement():
    options = Options()
    # options.add_argument('--headless')  # ブラウザを開かないように設定
    options.add_experimental_option("prefs", {
        "download.default_directory": statement_dir,
        "download.prompt_for_download": False,
        "plugin.always_open_pdf_externally": True
    })

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),options=options)

    # 楽天カードの利用明細についてのページを開く
    driver.get('https://www.rakuten-card.co.jp/e-navi/index.xhtml')
    time.sleep(3)

    # ログイン画面のユーザー欄にユーザIDを入力
    driver.find_element(By.NAME,'u').send_keys(env.RAKUTEN_ID)
    # ログイン画面のパスワード欄にパスワードを入力
    driver.find_element(By.NAME,'p').send_keys(env.RAKUTEN_PW)
    # ログインボタンを押下する
    driver.find_element(By.ID,'loginButton').click()
    # 利用明細画面に遷移する
    driver.find_element(By.LINK_TEXT,'明細を見る').click()
    # 利用明細CSVのダウンロードリンク先を取得
    tag = driver.find_element(By.CSS_SELECTOR,'.stmt-c-btn-dl.stmt-csv-btn')
    href = tag.get_attribute('href')

    # Chromeからクッキーデータを得る
    c = {}
    for cookie in driver.get_cookies():
        c[cookie['name']] = cookie['value']
    # requestsを利用してデータのダウンロード
    r = requests.get(href, cookies=c)
    with open(statement_addr, 'wb') as f:
        f.write(r.content)

    # 30秒待ってから終了する
    time.sleep(10)
    driver.quit()

# カード明細データを編集する
def get_statement():
    statememt_df = pd.read_csv(statement_addr)

    # 必要な列のみ取得
    statememt_df = statememt_df[["利用日","利用店名・商品名","利用金額"]]

    # 支払日を一括で追加 当月25日
    payment_date = datetime.now().strftime("%Y/%m/25")
    statememt_df["支払い日"] = payment_date

    # 家賃追加 使用日は翌月1日とする
    statememt_df.loc[""] = [rent_use_date,"家賃",env.rent,payment_date]

    # 利用先が初出か判定
    unregistered_use_targets = statememt_df.groupby("利用店名・商品名").count().index.values.tolist()
    existing_use_targets = get_existing_use_targets()
    for x in existing_use_targets:
        if x in unregistered_use_targets:
            unregistered_use_targets.remove(x)

    # 初出の利用先があれば利用先マスターに登録
    if unregistered_use_targets:
        add_use_targets(unregistered_use_targets)
    return statememt_df


# LINEでメッセージを送信する
def send_msg(msg,is_test=False):
    #APIのURL
    api_url = "https://notify-api.line.me/api/notify"
    send_data = {"message": msg}

    for LINE_NOTIFY_TOKEN in env.LINE_NOTIFY_TOKENs:
        requests.post(
            api_url,
            headers={"Authorization" : "Bearer "+LINE_NOTIFY_TOKEN},
            data=send_data
        )
        if is_test: break


def conn_supabase():
    ip = sb.IP
    port = sb.PORT
    dbname = sb.DB
    user = sb.USER
    pw = sb.PW
    return f"host={ip} port={port} dbname={dbname} user={user} password={pw}"

# 現在の利用先リストを取得する
def get_existing_use_targets():
    sql = f"""
        SELECT use_target
        FROM household_expenses.ms_use_target
    """

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            data = cur.fetchall()

    existing_use_targets = [use_target[0].replace("\"","") for use_target in data]

    return existing_use_targets

# 利用先を追加する
def add_use_targets(use_targets):
    use_targets = [[use_target] for use_target in use_targets]
    sql = f"""
        INSERT INTO household_expenses.ms_use_target
            (use_target)
        VALUES {str(use_targets).replace("[","(").replace("]",")")[1:-1]}
    """

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

# 支払い履歴を追加する
def insert_pay_history(pay_history_df):
    pay_history_list = pay_history_df.to_numpy().tolist()

    insert_sql = []
    for row in pay_history_list:
        insert_sql.append(str(row).replace("[","(").replace("]",")"))

    insert_sql = ",".join(insert_sql)

    sql = """
        INSERT INTO household_expenses.tr_pay_history
            (use_date,use_target,price,payment_date)
        VALUES
    """

    sql += insert_sql

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

# 各人の支払額を取得
def get_monthly_payment_amount():
    sql = f"""
        SELECT {person[0]["aa"]},{person[1]["aa"]}
        FROM household_expenses.v_monthly_payment_amount_each_person
        WHERE month = \'{datetime.now().strftime("%Y/%m")}\'
    """

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            payment_amount = cur.fetchone()

    return payment_amount

# カテゴリーごとの金額を取得
def get_monthly_expenses_each_category():
    sql = f"""
        SELECT category,amount
        FROM household_expenses.v_monthly_expenses_each_category
        WHERE month = \'{datetime.now().strftime("%Y/%m")}\'
    """

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            data = cur.fetchall()

    return data

# 当月のデータが登録されているか確認する 家賃のデータ有無で確認
def exists_cur_month_data():
    sql = f"""
        SELECT use_date,use_target
        FROM household_expenses.tr_pay_history
        WHERE use_date = \'{rent_use_date}\'
        and use_target = \'家賃\'
    """

    with psycopg2.connect(conn_supabase()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            data = cur.fetchall()

    return True if data else False

main()