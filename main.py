import os
import asyncio
import asyncpg
import httpx
from dotenv import load_dotenv

import requests
load_dotenv()

DB_CONFIG = {
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "database": os.getenv("PGDATABASE"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT"),
}

IIKO_ORG_ID = os.getenv("ORG_ID")


async def fetch_token():
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        row = await conn.fetchrow("SELECT token FROM iiko_access_tokens ORDER BY created_at DESC LIMIT 1")
        await conn.close()
        return row["token"] if row else None
    except Exception as e:
        print(f"❌ Ошибка подключения к базе или получения токена: {e}")
        return None

def fetch_terminal_groups(token):
    url = "https://api-ru.iiko.services/api/1/terminal_groups"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    json_data = {
        "organizationIds": [IIKO_ORG_ID]
    }

    response = requests.post(url, headers=headers, json=json_data)
    response.raise_for_status()

    data = response.json()

    try:
        items = data["terminalGroups"][0]["items"]
        for item in items:
            print(f"📟 Терминальная группа: {item['name']} | ID: {item['id']}")
        return [item["id"] for item in items]
    except Exception as e:
        print(f"❌ Ошибка извлечения ID терминальных групп: {e}")
        return []



def fetch_stop_list_raw(token, terminal_group_ids):
    url = "https://api-ru.iiko.services/api/1/stop_lists"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    json_data = {
        "organizationIds": [IIKO_ORG_ID],
        "terminalGroupIds": terminal_group_ids  # ← исправлено: terminalGroupIds, не terminalGroupsIds
    }

    response = requests.post(url, headers=headers, json=json_data)
    print(f"\n📡 Ответ от stopLists:\n{response.status_code}\n{response.text}")

    if response.status_code == 200:
        try:
            data = response.json()
            # Достаём стоп-лист всех терминальных групп
            items = data["terminalGroupStopLists"][0]["items"][0]["items"]
            return items
        except Exception as e:
            print(f"❌ Ошибка при парсинге стоп-листа: {e}")
            return []
    else:
        print("❌ Запрос стоп-листа не удался.")
        return []

async def map_stoplist_with_db(db_config, stoplist_items):
    conn = await asyncpg.connect(**db_config)

    # Собираем список всех sku из стоп-листа
    skus = [item["sku"] for item in stoplist_items]
    query = "SELECT code, name FROM nomenclature WHERE code = ANY($1)"
    rows = await conn.fetch(query, skus)

    await conn.close()

    # Словарь: sku → name
    sku_name_map = {row["code"]: row["name"] for row in rows}

    # Формируем результат
    result = []
    for item in stoplist_items:
        name = sku_name_map.get(item["sku"], "[НЕ НАЙДЕНО В БД]")
        item["name"] = name  # ← Вот это важно!
        result.append(f"{name} | SKU: {item['sku']} | Остаток: {item['balance']}")
    
    return result



async def sync_stoplist_with_db(stoplist_items):
    conn = await asyncpg.connect(**DB_CONFIG)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_stoplist (
            sku TEXT PRIMARY KEY,
            balance REAL,
            name TEXT
        );
    """)

    # Получение текущих SKU и названий из таблицы
    current_rows = await conn.fetch("SELECT sku, name FROM active_stoplist;")
    current_skus = {row["sku"]: row["name"] for row in current_rows}

    # SKU из API
    incoming_skus = {item["sku"]: item["name"] for item in stoplist_items}

    # Новые
    new_items = [(sku, name) for sku, name in incoming_skus.items() if sku not in current_skus]
    # Уже были
    existing_items = [(sku, name) for sku, name in incoming_skus.items() if sku in current_skus]
    # Удаляемые
    to_delete = [sku for sku in current_skus if sku not in incoming_skus]

    # Вставка новых
    for sku, name in new_items:
        item = next(i for i in stoplist_items if i["sku"] == sku)
        await conn.execute("""
            INSERT INTO active_stoplist (sku, balance, name) VALUES ($1, $2, $3);
        """, sku, item["balance"], name)

    # Удаление исчезнувших
    if to_delete:
        await conn.execute("""
            DELETE FROM active_stoplist WHERE sku = ANY($1);
        """, to_delete)

    await conn.close()

    # 🧾 Логируем результат
    print("\nНовые блюда в стоп-листе 🚫")
    for _, name in new_items:
        print(f"▫️ {name}")
    print("\nУже в стоп-листе")
    for _, name in existing_items:
        print(f"▫️ {name}")
    print("\n#стоплист")

    print(f"\n✅ Синхронизация завершена. Добавлено: {len(new_items)}, удалено: {len(to_delete)}")



async def main():
    token = await fetch_token()
    if token:
        print(f"✅ Актуальный токен: {token}")
        terminal_group_ids = fetch_terminal_groups(token)
        stoplist_items = fetch_stop_list_raw(token, terminal_group_ids)
        mapped = await map_stoplist_with_db(DB_CONFIG, stoplist_items)
        print("\n🧾 Сопоставленные стоп-позиции:")
        for line in mapped:
            print("•", line)
        await sync_stoplist_with_db(stoplist_items)  # ← Добавлено сюда
    else:
        print("❌ Токен не найден.")

if __name__ == "__main__":
    asyncio.run(main())

