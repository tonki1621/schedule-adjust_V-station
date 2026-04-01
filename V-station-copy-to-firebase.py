import json
import requests
from google.oauth2 import service_account
from google.cloud import firestore
import hashlib

# 定数設定
GAS_URL = "https://script.google.com/macros/s/AKfycby7hAc1_dhSQ_tJzSiJeSc2Ez7pgaeVTrVL5fOIZPNNZ-_YLke236yGgCgj3yijhQHh/exec"
FIREBASE_KEY_PATH = "schedule-adjust-v-station-firebase-adminsdk-fbsvc-4ff4ea3c98.json" # ローカルのサービスアカウントキーのパス

# ==========================================
# ユーティリティ関数
# ==========================================
def hash_secret(text):
    if not text:
        return ""
    return hashlib.sha256(str(text).encode()).hexdigest()

def get_firestore_client():
    with open(FIREBASE_KEY_PATH, "r", encoding="utf-8") as f:
        key_dict = json.load(f)
    if "private_key" in key_dict:
        key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(key_dict)
    return firestore.Client(credentials=creds, project=key_dict["project_id"])

def delete_collection(coll_ref, batch_size=100):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    for doc in docs:
        doc.reference.delete()
        deleted += 1
    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

# ==========================================
# メイン同期処理
# ==========================================
def main():
    print("GASからデータを取得中...")
    res = requests.get(f"{GAS_URL}?action=get_all_data_raw")
    if res.status_code != 200:
        print("GASからのデータ取得に失敗しました。")
        return
        
    data = res.json().get("data", {})
    users_raw = data.get("users", [])
    fixed_raw = data.get("fixed_schedule", [])
    events_raw = data.get("events", [])
    responses_raw = data.get("responses", [])
    master_config_raw = data.get("master_config", [])

    db = get_firestore_client()
    
    # 💡【最重要】Firestoreを消去する前に、既存ユーザーの秘匿データ（PIN等）をメモリにバックアップ
    print("Firestoreの既存ユーザーの秘匿データをバックアップ中...")
    existing_users = {doc.id: doc.to_dict() for doc in db.collection("users").stream()}
    
    print("Firestoreの既存データを初期化（削除）中...")
    delete_collection(db.collection("users"))
    delete_collection(db.collection("events"))
    delete_collection(db.collection("responses"))
    
    # --- Users & Fixed Schedule の復元 ---
    print("Usersデータの同期中...")
    fixed_map = {}
    if len(fixed_raw) > 1:
        for row in fixed_raw[1:]:
            uid = str(row[0])
            day = str(row[1])
            bdata = str(row[2]).lstrip("'")
            if uid not in fixed_map:
                fixed_map[uid] = {}
            fixed_map[uid][day] = bdata

    if len(users_raw) > 1:
        for row in users_raw[1:]:
            uid = str(row[0])
            raw_name = str(row[1])
            raw_pin = str(row[2]).lstrip("'")
            role = str(row[3]) if len(row) > 3 and row[3] else "guest"
            group_1 = str(row[4]) if len(row) > 4 else ""
            group_2 = str(row[5]) if len(row) > 5 else ""
            group_3 = str(row[6]) if len(row) > 6 else ""
            group_4 = str(row[7]) if len(row) > 7 else ""
            raw_secret = str(row[8]) if len(row) > 8 else ""
            raw_cal_url = str(row[9]) if len(row) > 9 else ""
            discord_id = str(row[10]) if len(row) > 10 else ""

            # ==========================================
            # 🛡️ マスキングされたデータの保護（リストア）ロジック
            # ==========================================
            if uid in existing_users:
                # 1. PINの保護（GAS側がダミーならFirestoreの元の値を維持）
                if raw_pin == "ENCRYPTED_PIN":
                    final_pin = existing_users[uid].get("pin", "")
                else:
                    final_pin = hash_secret(raw_pin)

                # 2. 秘密の合言葉の保護
                if raw_secret == "SET_BY_USER":
                    final_secret = existing_users[uid].get("secret_word", "")
                else:
                    final_secret = hash_secret(raw_secret)

                # 3. カレンダーURLの保護
                if raw_cal_url == "LINKED":
                    final_cal_url = existing_users[uid].get("calendar_url", "")
                else:
                    final_cal_url = raw_cal_url
            else:
                # Firestoreにデータが無い（スプレッドシートから手動で新規追加された）場合
                final_pin = hash_secret(raw_pin)
                final_secret = hash_secret(raw_secret)
                final_cal_url = raw_cal_url

            user_doc = {
                "user_id": uid,
                "name": raw_name,
                "pin": final_pin,
                "role": role,
                "group_1": group_1,
                "group_2": group_2,
                "group_3": group_3,
                "group_4": group_4,
                "secret_word": final_secret,
                "calendar_url": final_cal_url,
                "discord_id": discord_id,
                "fixed_schedule": fixed_map.get(uid, {})
            }
            db.collection("users").document(uid).set(user_doc)

    # --- Events の復元 ---
    print("Eventsデータの同期中...")
    if len(events_raw) > 1:
        for row in events_raw[1:]:
            ev_id = str(row[0])
            ev_doc = {
                "event_id": ev_id,
                "title": str(row[1]),
                "start_date": str(row[2])[:10] if row[2] else "",
                "end_date": str(row[3])[:10] if row[3] else "",
                "status": str(row[4]),
                "start_time_idx": int(row[5]) if row[5] else 0,
                "end_time_idx": int(row[6]) if row[6] else 0,
                "description": str(row[7]) if len(row) > 7 else "",
                "type": str(row[8]) if len(row) > 8 else "time",
                "options_name": str(row[9]) if len(row) > 9 else "",
                "close_time": str(row[10]) if len(row) > 10 else "",
                "auto_close": bool(row[11]) if len(row) > 11 else False,
                "target_scope": str(row[12]) if len(row) > 12 else "",
                "is_private": bool(row[13]) if len(row) > 13 else False
            }
            db.collection("events").document(ev_id).set(ev_doc)

    # --- Responses の復元 ---
    print("Responsesデータの同期中...")
    res_map = {}
    if len(responses_raw) > 1:
        for row in responses_raw[1:]:
            ev_id = str(row[0])
            uid = str(row[1])
            date_val = str(row[2])[:10] if row[2] and "T" in str(row[2]) else str(row[2])
            bdata = str(row[3]).lstrip("'")
            comment = str(row[4]) if len(row) > 4 else ""
            cell_details = str(row[5]) if len(row) > 5 else "{}"
            
            doc_id = f"{ev_id}_{uid}"
            if doc_id not in res_map:
                res_map[doc_id] = {
                    "event_id": ev_id,
                    "user_id": uid,
                    "cell_details": cell_details,
                    "comment": comment,
                    "responses": []
                }
            
            res_map[doc_id]["responses"].append({
                "date": date_val,
                "binary_data": bdata
            })
            if comment and not res_map[doc_id]["comment"]:
                res_map[doc_id]["comment"] = comment

    for doc_id, data in res_map.items():
        db.collection("responses").document(doc_id).set(data)

    # --- Config の復元 ---
    print("Configデータの同期中...")
    g1, g3 = [], []
    if len(master_config_raw) > 1:
        for row in master_config_raw[1:]:
            if len(row) > 0 and str(row[0]).strip():
                g1.append(str(row[0]).strip())
            if len(row) > 1 and str(row[1]).strip():
                g3.append(str(row[1]).strip())
    
    db.collection("config").document("master").set({
        "g1": g1 if g1 else ["なかもず", "もりのみや", "すぎもと", "あべの", "りんくう"],
        "g3": g3 if g3 else ["卒業生ネットワーク関係者"]
    })

    print("🎉 同期完了！")

if __name__ == "__main__":
    main()
