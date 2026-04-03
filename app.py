import streamlit as st
import pandas as pd
import json
import time
import urllib.parse
import os
from datetime import datetime, timedelta
import requests
import streamlit.components.v1 as components
import numpy as np
from google.oauth2 import service_account
from google.cloud import firestore
import threading
import hashlib
import random
import string

# ==========================================
# Firebase の初期化 ＆ 高速連携関数
# ==========================================

def hash_secret(text):
    if not text:
        return ""
    return hashlib.sha256(str(text).encode()).hexdigest()

def generate_custom_id(prefix="EV"):
    now_str = datetime.now().strftime("%y%m%d-%H%M")
    rand_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}-{now_str}-{rand_str}"

@st.cache_resource
def get_firestore_client():
    key_dict = dict(st.secrets["firebase"])
    if "private_key" in key_dict:
        key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
        
    creds = service_account.Credentials.from_service_account_info(key_dict)
    db = firestore.Client(credentials=creds, project=key_dict["project_id"])
    return db

db = get_firestore_client()

def backup_to_gas_async(action, payload=None):
    def _call():
        try:
            p = payload or {}
            p["action"] = action
            requests.post(GAS_URL, json=p)
        except Exception as e:
            print(f"GAS backup failed: {e}")
    threading.Thread(target=_call).start()

def save_response_hybrid(payload):
    try:
        event_id = payload["event_id"]
        user_id = payload["user_id"]
        
        cell_details_dict = json.loads(payload.get("cell_details", "{}"))
        if payload.get("comment"):
            cell_details_dict["global_comment"] = payload["comment"]
            
        final_cell_details_str = json.dumps(cell_details_dict, separators=(',', ':'))
        payload["cell_details"] = final_cell_details_str
        
        doc_ref = db.collection("responses").document(f"{event_id}_{user_id}")
        data = {
            "event_id": event_id,
            "user_id": user_id,
            "cell_details": final_cell_details_str,
            "responses": payload.get("responses", []), 
            "updated_at": firestore.SERVER_TIMESTAMP
        }
        doc_ref.set(data)
    except Exception as e:
        st.error(f"Firestoreへの保存に失敗しました: {e}")
        return False

    # 💡 確実な同期のためpayloadで包む
    backup_to_gas_async("submit_binary_response", {"payload": payload})
    return True

def get_app_data_from_firestore(user):
    user_id = str(user.get("user_id", ""))
    
    all_users = [doc.to_dict() for doc in db.collection("users").stream()]
    user_map = {str(u["user_id"]): u for u in all_users}
    
    answered_ids = set()
    if user_id:
        ans_docs = db.collection("responses").where("user_id", "==", user_id).stream()
        for doc in ans_docs:
            answered_ids.add(doc.to_dict().get("event_id"))

    now = datetime.now()
    active_events = []
    
    user_groups = [g.strip() for g in str(user.get("group_1","")).split(",") + str(user.get("group_2","")).split(",") + str(user.get("group_3","")).split(",") + str(user.get("group_4","")).split(",") if g.strip()]
    
    all_events_docs = db.collection("events").stream()
    for doc in all_events_docs:
        ev = doc.to_dict()
        if ev.get("status") not in ["open", "closed"]: 
            continue
        
        ev_type = ev.get("type") or ev.get("event_type", "time")
        ev_close_time = ev.get("close_time") or ev.get("deadline", "")
        
        if ev.get("status") == "open" and ev.get("auto_close") and ev_close_time:
            try:
                dl_dt = pd.to_datetime(ev_close_time, errors='coerce')
                if pd.notna(dl_dt):
                    dl_dt = dl_dt.tz_localize(None)
                    if now > dl_dt:
                        ev["status"] = "closed"
                        db.collection("events").document(ev["event_id"]).update({"status": "closed"})
                        # 💡 確実な同期のためpayloadで包む
                        backup_to_gas_async("update_event_status", {"payload": {"event_id": ev["event_id"], "status": "closed"}})
            except: 
                pass

        is_target = True
        scope_str = ev.get("target_scope", "")
        if scope_str and scope_str.startswith("{"):
            try:
                scope = json.loads(scope_str)
                t_groups = scope.get("groups", [])
                t_users = scope.get("users", [])
                if t_groups or t_users:
                    in_group = any(g in user_groups for g in t_groups)
                    in_user = user_id in t_users
                    if not in_group and not in_user:
                        is_target = False
            except: 
                pass
            
        if is_target:
            ev["is_answered"] = ev["event_id"] in answered_ids
            active_events.append(ev)

    return all_users, active_events, user_map

def fetch_responses_for_event(event_id, user_map):
    docs = db.collection("responses").where("event_id", "==", event_id).stream()
    flat_responses = []
    for doc in docs:
        data = doc.to_dict()
        uid = str(data.get("user_id"))
        uinfo = user_map.get(uid, {})
        
        cell_details_str = data.get("cell_details", "{}")
        try:
            cell_details_dict = json.loads(cell_details_str)
            comment = cell_details_dict.get("global_comment", data.get("comment", ""))
        except:
            comment = data.get("comment", "")
        
        for r in data.get("responses", []):
            b_data = r.get("binary_data") or r.get("binary", "")
            flat_responses.append({
                "user_id": uid,
                "user_name": uinfo.get("name", "不明"),
                "group_1": uinfo.get("group_1", ""),
                "group_2": uinfo.get("group_2", ""),
                "group_3": uinfo.get("group_3", ""),
                "group_4": uinfo.get("group_4", ""),
                "date": r.get("date"),
                "binary_data": b_data,
                "comment": comment,
                "cell_details": cell_details_str
            })
    return flat_responses

# ==========================================
# Streamlit 初期設定 & コンポーネント
# ==========================================
st.set_page_config(page_title="V-Sync by もっきゅー", layout="wide")
APP_BASE_URL = "https://schedule-adjust-v-station.streamlit.app/"

st.markdown("""
    <style>
        st.markdown("""
    <style>
        /* スマホ時に画面を広く使えるよう余白を最適化 */
        @media (max-width: 650px) {
            .main .block-container,
            div[data-testid="stAppViewBlockContainer"] {
                padding-left: 1rem !important; 
                padding-right: 1rem !important;
                padding-top: 1rem !important;
            }
            iframe { max-width: 100vw !important; width: 100% !important; }
        }
        
        .stDeployStatus, [data-testid="stStatusWidget"] label { display: none !important; }
        [data-testid="stStatusWidget"] { visibility: visible !important; display: flex !important; position: fixed !important; top: 50% !important; left: 50% !important; transform: translate(-50%, -50%) !important; background: rgba(255, 255, 255, 0.95) !important; color: #333 !important; padding: 20px 40px !important; border-radius: 12px !important; z-index: 999999 !important; box-shadow: 0 8px 24px rgba(0,0,0,0.15) !important; border: 2px solid #4CAF50 !important; text-align: center !important; justify-content: center !important; }
        [data-testid="stStatusWidget"]::after { content: "⏳ 通信中 \\A 処理しています..."; white-space: pre-wrap; font-size: 20px !important; font-weight: bold !important; line-height: 1.5 !important; }
        @media (max-width: 600px) { [data-testid="stStatusWidget"] { padding: 15px 20px !important; width: 80% !important; } [data-testid="stStatusWidget"]::after { font-size: 16px !important; } }
        .stApp, .stApp [data-testid="stAppViewBlockContainer"], div[data-testid="stVerticalBlock"], div[data-testid="stForm"], iframe { opacity: 1 !important; transition: none !important; filter: none !important; }
        .user-header { display: flex; align-items: center; justify-content: space-between; background: #f8f9fa; padding: 10px 20px; border-radius: 8px; border-left: 5px solid #4CAF50; margin-bottom: 20px; }
        .event-desc { background: #fff8e1; padding: 15px; border-radius: 8px; border-left: 4px solid #ffc107; margin-bottom: 20px; font-size: 14px; line-height: 1.6; }
        .tt-day-header { font-size: 16px; font-weight: bold; background: #4CAF50; color: white; padding: 8px; border-radius: 6px; text-align: center; }
        .tt-time-cell { font-size: 14px; font-weight: bold; background: #f0f2f6; padding: 10px; border-radius: 6px; text-align: center; border-left: 4px solid #4CAF50;}
        .tt-time-sub { font-size: 11px; color: #666; font-weight: normal; }
        .status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #4CAF50, #45a049); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(76,175,80,0.3); }
        .af-status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #2196F3, #1976D2); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(33,150,243,0.3); }
        .status-off { color: #9e9e9e; background: #ffffff; padding: 4px 0; border-radius: 6px; border: 1px dashed #d0d0d0; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block;}
        .tt-table [data-testid="stCheckbox"] { justify-content: center; margin: 0 !important; padding: 0 !important; width: 100% !important;}
    </style>
""", unsafe_allow_html=True)

GAS_URL = "https://script.google.com/macros/s/AKfycby7hAc1_dhSQ_tJzSiJeSc2Ez7pgaeVTrVL5fOIZPNNZ-_YLke236yGgCgj3yijhQHh/exec"

os.makedirs("rt_editor", exist_ok=True)
with open("rt_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:sans-serif;margin:0;padding:0;background:transparent;}.editor-container{border:1px solid #ccc;border-radius:6px;overflow:hidden;background:#fff;}.toolbar{background:#f8f9fb;padding:6px;border-bottom:1px solid #ccc;display:flex;gap:5px;flex-wrap:wrap;align-items:center;}.toolbar button{background:#fff;border:1px solid #ccc;border-radius:4px;padding:4px 10px;font-size:13px;cursor:pointer;color:#333;transition:0.2s;}.toolbar button:hover{background:#e9ecef;}textarea{width:100%;height:120px;border:none;padding:10px;font-size:14px;resize:vertical;outline:none;box-sizing:border-box;font-family:inherit;line-height:1.5;}</style></head><body><div class="editor-container"><div class="toolbar"><button onclick="insertTag('<b>', '</b>')" title="太字"><b>B</b> 太字</button><button onclick="insertTag('<i>', '</i>')" title="斜体"><i>I</i> 斜体</button><div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div><button onclick="insertRed()" title="赤文字"><span style="color:#FF4B4B; font-weight:bold;">A</span> 赤</button><button onclick="insertBlue()" title="青文字"><span style="color:#2196F3; font-weight:bold;">A</span> 青</button><div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div><button onclick="insertLink()" title="リンク">🔗 リンク追加</button></div><textarea id="editor" placeholder="📝 イベントの説明や注意事項を入力..."></textarea></div><script>function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); } function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); } function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); } const editor = document.getElementById('editor'); let timer; function sendValue() { setComponentValue(editor.value); } function insertTag(startTag, endTag) { const start = editor.selectionStart; const end = editor.selectionEnd; const val = editor.value; const selected = val.substring(start, end); editor.value = val.substring(0, start) + startTag + selected + endTag + val.substring(end); editor.focus(); editor.selectionStart = start + startTag.length; editor.selectionEnd = end + startTag.length; sendValue(); } function insertRed() { insertTag("<span style='color:#FF4B4B; font-weight:bold;'>", "</span>"); } function insertBlue() { insertTag("<span style='color:#2196F3; font-weight:bold;'>", "</span>"); } function insertLink() { const url = prompt('リンク先のURLを入力', 'https://'); if (url) { const text = prompt('表示するテキストを入力', 'こちらをクリック'); if (text) { const linkTag = `<a href='${url}' target='_blank'>${text}</a>`; const start = editor.selectionStart; const val = editor.value; editor.value = val.substring(0, start) + linkTag + val.substring(editor.selectionEnd); sendValue(); } } } editor.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(sendValue, 500); }); editor.addEventListener('blur', sendValue); window.addEventListener("message", function(event) { if (event.data.type === "streamlit:render") { sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.body.scrollHeight + 15}); } }); init();</script></body></html>""")
rt_editor = components.declare_component("rt_editor", path="rt_editor")

os.makedirs("options_editor", exist_ok=True)
with open("options_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{margin:0;font-family:sans-serif;}.opt-card{background:#fff;border:1px solid #e0e0e0;border-radius:12px;padding:15px;margin-bottom:15px;box-shadow:0 2px 5px rgba(0,0,0,0.05);}.opt-title{font-size:18px;font-weight:bold;color:#2e7d32;margin-bottom:15px;text-align:center;}.btn-group{display:flex;gap:12px;}.opt-btn{flex:1;padding:20px 0;border-radius:12px;border:2px solid #ddd;background:#fff;font-size:18px;font-weight:bold;cursor:pointer;transition:all 0.2s cubic-bezier(0.175, 0.885, 0.32, 1.275);color:#555;text-align:center;}.opt-btn[data-v="1"].active{background:#4CAF50;color:#fff;border-color:#4CAF50;box-shadow:0 6px 12px rgba(76,175,80,0.4);transform:translateY(-3px);}.opt-btn[data-v="2"].active{background:#FFEB3B;color:#333;border-color:#FBC02D;box-shadow:0 6px 12px rgba(255,235,59,0.4);transform:translateY(-3px);}.opt-btn[data-v="0"].active{background:#f5f5f5;color:#777;border-color:#ccc;transform:translateY(-3px);}#submit-btn{width:100%;padding:18px;background-color:#FF4B4B;color:white;border:none;border-radius:12px;font-size:20px;cursor:pointer;font-weight:bold;box-shadow:0 6px 12px rgba(0,0,0,0.15);margin-top:10px;transition:0.2s;}#submit-btn:hover{background-color:#e63946;transform:translateY(-2px);}textarea{width:100%;padding:15px;border:1px solid #ccc;border-radius:12px;font-family:inherit;font-size:16px;margin-bottom:10px;resize:vertical;box-sizing:border-box;}</style></head><body><div id="content"></div><script>function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); } function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); } function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); } let optsData = []; let myComment = ""; window.addEventListener("message", function(event) { if (event.data.type === "streamlit:render") { const args = event.data.args; if(window.lastEventId === args.eventId && window.lastSaveTs === args.saveTs) return; window.lastEventId = args.eventId; window.lastSaveTs = args.saveTs; const opts = args.options; const myAnsBin = args.myAnsBin; myComment = args.myComment || ""; const isClosed = args.isClosed; let html = ""; optsData = []; opts.forEach((opt, i) => { let v = i < myAnsBin.length ? parseInt(myAnsBin[i]) : 0; optsData.push(v); let pointerEv = isClosed ? "pointer-events:none; opacity:0.7;" : ""; html += `<div class="opt-card" style="${pointerEv}"><div class="opt-title">📅 ${opt}</div><div class="btn-group" id="group-${i}"><button class="opt-btn ${v===0 ? 'active':''}" data-v="0" onclick="setOpt(${i}, 0)">× 不可</button><button class="opt-btn ${v===2 ? 'active':''}" data-v="2" onclick="setOpt(${i}, 2)">△ 未定</button><button class="opt-btn ${v===1 ? 'active':''}" data-v="1" onclick="setOpt(${i}, 1)">◯ 可</button></div></div>`; }); if(!isClosed) { html += `<div class="opt-card"><div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント</div><textarea id="comment-box" rows="2" placeholder="遅刻・早退などの連絡事項">${myComment}</textarea><button id="submit-btn" onclick="submitData()">✅ 回答を保存して提出</button></div>`; } else { html += `<div class="opt-card"><div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント</div><div style="padding:15px; background:#eee; border-radius:12px; min-height:50px; font-size:16px;">${myComment}</div></div>`; } document.getElementById("content").innerHTML = html; setTimeout(() => sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.getElementById('content').scrollHeight + 50}), 150); } }); window.setOpt = function(idx, val) { optsData[idx] = val; const btns = document.getElementById('group-' + idx).querySelectorAll('.opt-btn'); btns.forEach(b => b.classList.remove('active')); document.getElementById('group-' + idx).querySelector(`[data-v="${val}"]`).classList.add('active'); }; window.submitData = function() { const btn = document.getElementById("submit-btn"); btn.innerText = "⏳ 保存処理中..."; btn.style.pointerEvents = "none"; const comment = document.getElementById("comment-box").value; setComponentValue({ trigger_save: true, binary: optsData.join(''), comment: comment, ts: Date.now() }); }; init();</script></body></html>""")
options_editor = components.declare_component("options_editor", path="options_editor")

if not os.path.exists("custom_editor_v4"):
    os.makedirs("custom_editor_v4", exist_ok=True)
    with open("custom_editor_v4/index.html", "w", encoding="utf-8") as f:
        f.write("""
        <!DOCTYPE html><html><head><meta charset="utf-8"><style>
        body{margin:0;font-family:sans-serif;} *{box-sizing:border-box;}
        .pen-btn { padding: 0; border-radius: 50%; width: 45px; height: 45px; border: none; cursor: pointer; font-weight: bold; font-size: 14px; transition: transform 0.2s, box-shadow 0.2s; display: flex; align-items: center; justify-content: center; box-shadow: 0 2px 4px rgba(0,0,0,0.15); margin: 0 auto; }
        .pen-btn:hover { opacity: 0.8; }
        .pen-btn.active { border: 3px solid #333 !important; transform: scale(1.1); box-shadow: 0 4px 8px rgba(0,0,0,0.3); }
        
        #detail-modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:999999;justify-content:center;align-items:center;backdrop-filter:blur(2px);}
        .modal-content{background:#fff;width:320px;padding:20px;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,0.2);position:relative;}
        .modal-title{font-size:16px;font-weight:bold;color:#333;margin-bottom:10px;border-bottom:2px solid #4CAF50;padding-bottom:5px;}
        .modal-label{font-size:12px;font-weight:bold;color:#666;margin-top:15px;display:block;}
        .modal-select, .modal-input{width:100%;padding:8px;margin-top:5px;border:1px solid #ccc;border-radius:6px;font-size:14px;}
        .status-switch{display:flex;gap:8px;margin-top:5px;}
        .sw-btn{flex:1;padding:8px;border:1px solid #ddd;border-radius:6px;cursor:pointer;font-size:13px;font-weight:bold;background:#f9f9f9;color:#555;transition:0.2s;}
        .sw-btn.active[data-v="1"]{background:#4CAF50;color:white;border-color:#4CAF50;}
        .sw-btn.active[data-v="2"]{background:#FFEB3B;color:#333;border-color:#FBC02D;}
        .sw-btn.active[data-v="0"]{background:#fff;color:#333;border-color:#999;}
        .modal-btns{display:flex;gap:10px;margin-top:20px;}
        .modal-btn-save{flex:1;background:#4CAF50;color:white;border:none;padding:12px;border-radius:6px;font-weight:bold;cursor:pointer;}
        .memo-icon{position:absolute;top:1px;right:2px;font-size:10px;line-height:1;filter:drop-shadow(1px 1px 1px rgba(255,255,255,0.8));pointer-events:none;}
        .c{position:relative;transition:filter 0.1s;}
        @keyframes pressAnim{0%{transform:scale(1);filter:brightness(1);} 100%{transform:scale(0.92);filter:brightness(0.8);box-shadow:inset 0 4px 8px rgba(0,0,0,0.3);}}
        .pressing{animation:pressAnim 0.4s forwards;z-index:100;}
        </style></head><body>
        
        <div id="palette" style="position:fixed; top:20px; right:30px; z-index:99999; background:rgba(255,255,255,0.95); border:1px solid #ddd; border-radius:12px; box-shadow:0 8px 24px rgba(0,0,0,0.2); padding:12px 8px; cursor:move; display:none; flex-direction:column; gap:12px; backdrop-filter: blur(8px);">
            <div style="font-size:12px; font-weight:bold; color:#666; text-align:center; pointer-events:none; user-select:none; margin-bottom:-4px;">🖊️ ペン</div>
            <button class="pen-btn active" onclick="window.setPen(1)" id="pen-1" style="background:#4CAF50; color:#fff;">可</button>
            <button class="pen-btn" onclick="window.setPen(2)" id="pen-2" style="background:#FFEB3B; color:#333;">未定</button>
            <button class="pen-btn" onclick="window.setPen(0)" id="pen-0" style="background:#fff; color:#333; border:1px solid #ccc; font-size:12px;">🧽<br>消す</button>
            <hr style="margin:0; border-top:1px solid #ddd;">
            <button class="pen-btn" onclick="window.setPen(-1)" id="pen--1" style="background:#9C27B0; color:#fff; border:2px solid #7B1FA2; font-size:10px; margin-top:0px;">📜<br>ｽｸﾛｰﾙ</button>
        </div>

        <div id="detail-modal">
            <div class="modal-content" id="modal-content-box">
                <div class="modal-title" id="modal-cell-title">詳細設定</div>
                <label class="modal-label">🚥 予定のステータス</label>
                <div class="status-switch">
                    <button class="sw-btn" data-v="1" onclick="setModalStatus(1)">◯ 可</button>
                    <button class="sw-btn" data-v="2" onclick="setModalStatus(2)">△ 未定</button>
                    <button class="sw-btn" data-v="0" onclick="setModalStatus(0)">× 不可</button>
                </div>
                <label class="modal-label">🏫 キャンパスの指定</label>
                <select id="modal-campus" class="modal-select">
                    <option value="">指定なし</option>
                    <option value="なかもず">なかもず</option>
                    <option value="すぎもと">すぎもと</option>
                    <option value="あべの">あべの</option>
                    <option value="りんくう">りんくう</option>
                    <option value="もりのみや">もりのみや</option>
                    <option value="その他/移動中">その他 / 移動中</option>
                </select>
                <label class="modal-label">📝 補足コメント (任意)</label>
                <input type="text" id="modal-note" class="modal-input" placeholder="例: 13:30に移動開始, 20分遅延">
                <div class="modal-btns">
                    <button class="modal-btn-save" onclick="saveModal()">💾 保存して閉じる</button>
                </div>
                <div style="text-align:center; font-size:10px; color:#999; margin-top:10px;">※枠外をタップでキャンセル</div>
            </div>
        </div>

        <div id="content"></div><script>
        function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); }
        function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); }
        function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); }
        
        let currentWeek = 0; let totalDays = 0; let numRows = 0; let unavailColRows = {};
        window.cellDetails = {}; let defaultCampus = "";
        let modalStatus = 1; let selectedMode = 1; let editingCell = null;

        const modalBg = document.getElementById('detail-modal');
        modalBg.addEventListener('mousedown', function(e) { if(e.target === this) closeModal(); });
        modalBg.addEventListener('touchstart', function(e) { if(e.target === this) closeModal(); }, {passive: true});

        window.setModalStatus = function(v) {
            modalStatus = v;
            document.querySelectorAll('.sw-btn').forEach(b => { b.classList.toggle('active', parseInt(b.dataset.v) === v); });
        };

        window.openModal = function(cell) {
            editingCell = cell; const r = cell.dataset.r; const c = cell.dataset.c; const key = `${r}_${c}`;
            const detail = window.cellDetails[key] || {campus: defaultCampus, note: ""};
            setModalStatus(parseInt(cell.dataset.v) || 1);
            document.getElementById('modal-campus').value = detail.campus || "";
            document.getElementById('modal-note').value = detail.note || "";
            document.getElementById('detail-modal').style.display = 'flex';
        };

        window.closeModal = function() {
            document.getElementById('detail-modal').style.display = 'none';
            if (editingCell) { editingCell.classList.remove('pressing'); editingCell = null; }
        };

        window.saveModal = function() {
            if(!editingCell) return;
            const r = editingCell.dataset.r; const c = editingCell.dataset.c; const key = `${r}_${c}`;
            const campus = document.getElementById('modal-campus').value; const note = document.getElementById('modal-note').value.trim();
            if(campus || note || modalStatus === 0) { window.cellDetails[key] = {campus: campus, note: note}; window.upd(editingCell, modalStatus); }
            else { delete window.cellDetails[key]; window.upd(editingCell, modalStatus); }
            closeModal();
        };

        window.upd = function(el, v) { 
            el.dataset.v = v; const key = `${el.dataset.r}_${el.dataset.c}`; let detail = window.cellDetails[key];
            if (v == 0) {
                if (detail && (detail.note === "バイト/サークル等" || detail.note === "バイト/私用")) { }
                else { delete window.cellDetails[key]; detail = null; }
            } else if (v == 1 || v == 2) {
                if (!detail && defaultCampus) { window.cellDetails[key] = {campus: defaultCampus, note: ""}; detail = window.cellDetails[key]; }
                if (detail && detail.campus === defaultCampus && !detail.note) { delete window.cellDetails[key]; detail = null; }
            }

            let campus = detail ? detail.campus : ((v == 1 || v == 2) ? defaultCampus : "");
            let note = detail ? detail.note : "";
            let bgImage = 'none'; let bgColor = '#fff';

            if (v == 1) bgColor = '#4CAF50';
            else if (v == 2) bgColor = '#FFEB3B';
            else if (v == 3) bgColor = '#e0e0e0';

            if (v == 1 || v == 2 || v == 3 || (v == 0 && (note === "バイト/サークル等" || note === "バイト/私用"))) {
                let cColor = (v == 3) ? 'rgba(255,255,255,0.7)' : 'rgba(255,255,255,0.3)';
                let cColorDark = (v == 3) ? 'rgba(0,0,0,0.1)' : 'rgba(0,0,0,0.15)';
                if (v == 0 && (note === "バイト/サークル等" || note === "バイト/私用")) { bgImage = `repeating-linear-gradient(45deg, transparent, transparent 3px, rgba(0,0,0,0.08) 3px, rgba(0,0,0,0.08) 6px), repeating-linear-gradient(-45deg, transparent, transparent 3px, rgba(0,0,0,0.08) 3px, rgba(0,0,0,0.08) 6px)`; }
                else if (campus === "すぎもと" || campus === "杉本") { bgImage = `repeating-linear-gradient(45deg, ${cColor}, ${cColor} 4px, transparent 4px, transparent 8px)`; }
                else if (campus === "あべの" || campus === "阿倍野") { bgImage = `repeating-linear-gradient(-45deg, ${cColorDark}, ${cColorDark} 4px, transparent 4px, transparent 8px)`; }
                else if (campus === "りんくう") { bgImage = `radial-gradient(circle, ${cColor} 3px, transparent 4px)`; }
                else if (campus === "もりのみや") { bgImage = `repeating-linear-gradient(90deg, ${cColor}, ${cColor} 4px, transparent 4px, transparent 8px)`; }
                else if (campus === "その他/移動中") { bgImage = `repeating-linear-gradient(45deg, ${cColor}, ${cColor} 2px, transparent 2px, transparent 4px), repeating-linear-gradient(-45deg, ${cColor}, ${cColor} 2px, transparent 2px, transparent 4px)`; }
                else if (v == 3 && !campus) { bgImage = `repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(255,255,255,.8) 4px, rgba(255,255,255,.8) 8px)`; }
            }
            el.style.background = bgColor; el.style.backgroundImage = bgImage;
            if (campus === "りんくう" && v !== 0) el.style.backgroundSize = '10px 10px'; else el.style.backgroundSize = 'auto';

            const existingIcon = el.querySelector('.memo-icon');
            const hasManualSetting = detail && (detail.note !== "" || (detail.campus && detail.campus !== defaultCampus));
            if (hasManualSetting) { if (!existingIcon) el.insertAdjacentHTML('beforeend', '<div class="memo-icon">💬</div>'); }
            else { if (existingIcon) existingIcon.remove(); }
        };
        
        window.renderWeek = function() {
            const start = currentWeek * 7; const end = start + 7;
            document.querySelectorAll('.day-col').forEach(el => {
                const c = parseInt(el.dataset.c); el.style.display = (c >= start && c < end) ? 'block' : 'none';
            });
            const btnPrev = document.getElementById('btn-prev'); const btnNext = document.getElementById('btn-next');
            if(btnPrev) btnPrev.disabled = (currentWeek === 0); if(btnNext) btnNext.disabled = (end >= totalDays);
            setTimeout(() => sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.body.scrollHeight + 50}), 150);
        };
        window.changeWeek = function(dir) { currentWeek += dir; window.renderWeek(); };
        
        window.doBulk = function(btnEl) {
            const val = document.getElementById('b-val').value;
            const sIdx = parseInt(document.getElementById('b-start').value); const eIdx = parseInt(document.getElementById('b-end').value);
            if(sIdx > eIdx) { alert('エラー：開始時刻は終了時刻より前に設定してください。'); return; }
            document.querySelectorAll('.b-day-chk').forEach(chk => { if(chk.checked) { const cIdx = parseInt(chk.value); for(let r = sIdx; r <= eIdx; r++) { const cell = document.querySelector(`[data-r="${r}"][data-c="${cIdx}"]`); if(cell) window.upd(cell, val); } } });
            const origText = btnEl.innerText; btnEl.innerText = "✅ 完了"; setTimeout(() => btnEl.innerText = origText, 1500);
        };
        window.doCopy = function(btnEl) {
            const srcIdx = parseInt(document.getElementById('c-src').value);
            let srcData = []; for(let r = 0; r < numRows; r++) { const cell = document.querySelector(`[data-r="${r}"][data-c="${srcIdx}"]`); srcData.push(cell ? cell.dataset.v : 0); }
            let copied = false;
            document.querySelectorAll('.c-tgt-chk').forEach(chk => { if(chk.checked) { const cIdx = parseInt(chk.value); if(cIdx !== srcIdx) { copied = true; for(let r = 0; r < numRows; r++) { const cell = document.querySelector(`[data-r="${r}"][data-c="${cIdx}"]`); if(cell) window.upd(cell, srcData[r]); } } } });
            if(!copied) { alert('コピー先を選択してください。'); return; }
            const origText = btnEl.innerText; btnEl.innerText = "✅ 完了"; setTimeout(() => btnEl.innerText = origText, 1500);
        };
        
        window.doTimetable = function(btnEl) {
            if(!unavailColRows || Object.keys(unavailColRows).length === 0) { alert('時間割が登録されていないか、対象日がありません。'); return; }
            for(let c = 0; c < totalDays; c++) {
                let key = String(c);
                if (unavailColRows[key]) {
                    unavailColRows[key].forEach(item => {
                        const r = (typeof item === 'object') ? item.row : item; const campus = (typeof item === 'object') ? item.campus : ""; const cell = document.querySelector(`[data-r="${r}"][data-c="${c}"]`);
                        if(cell) {
                            const cellKey = `${r}_${c}`;
                            if (campus === "💼 バイト/サークル等" || campus === "💼 バイト/私用") { window.cellDetails[cellKey] = {campus: "", note: "バイト/サークル等"}; window.upd(cell, 0); }
                            else if (campus) { window.cellDetails[cellKey] = {campus: campus, note: "定期授業"}; window.upd(cell, 3); }
                            else { window.upd(cell, 3); }
                        }
                    });
                }
            }
            const origText = btnEl.innerHTML; btnEl.innerHTML = "✅ 反映完了！"; setTimeout(() => btnEl.innerHTML = origText, 2000);
        };
        
        window.toggleList = function(id) { const el = document.getElementById(id); el.style.display = el.style.display === 'none' ? 'block' : 'none'; };
        document.addEventListener('click', function(e) { if(!e.target.closest('.ms-container')) { document.querySelectorAll('.ms-options').forEach(el => el.style.display = 'none'); } });

        window.setPen = function(mode) {
            selectedMode = mode;
            [-1, 0, 1, 2].forEach(m => { const b = document.getElementById('pen-' + m); if(b) b.classList.remove('active'); });
            document.getElementById('pen-' + mode).classList.add('active');
            const g = document.getElementById('g');
            if (g) { if (mode === -1) { g.style.touchAction = 'pan-x pan-y'; } else { g.style.touchAction = 'none'; } }
        };

        const palette = document.getElementById('palette'); let isDraggingPalette = false; let offsetX, offsetY;
        palette.addEventListener('mousedown', e => { if (e.target.tagName.toLowerCase() === 'button') return; isDraggingPalette = true; offsetX = e.clientX - palette.getBoundingClientRect().left; offsetY = e.clientY - palette.getBoundingClientRect().top; });
        document.addEventListener('mousemove', e => { if (!isDraggingPalette) return; palette.style.left = (e.clientX - offsetX) + 'px'; palette.style.top = (e.clientY - offsetY) + 'px'; palette.style.right = 'auto'; });
        document.addEventListener('mouseup', () => { isDraggingPalette = false; });
        palette.addEventListener('touchstart', e => { if (e.target.tagName.toLowerCase() === 'button') return; isDraggingPalette = true; const touch = e.touches[0]; offsetX = touch.clientX - palette.getBoundingClientRect().left; offsetY = touch.clientY - palette.getBoundingClientRect().top; }, {passive: false});
        document.addEventListener('touchmove', e => { if (!isDraggingPalette) return; const touch = e.touches[0]; palette.style.left = (touch.clientX - offsetX) + 'px'; palette.style.top = (touch.clientY - offsetY) + 'px'; palette.style.right = 'auto'; e.preventDefault(); }, {passive: false});
        document.addEventListener('touchend', () => { isDraggingPalette = false; });

        window.addEventListener("message", function(event) {
            if (event.data.type === "streamlit:render") {
                const args = event.data.args; 
                document.getElementById("content").innerHTML = args.html_code;
                totalDays = args.cols; numRows = args.rows; unavailColRows = args.unavailColRows || {};
                window.cellDetails = args.cellDetails || {}; defaultCampus = args.defaultCampus || "";
                document.getElementById('pen-1').innerHTML = defaultCampus ? `可<br><span style='font-size:9px;'>(${defaultCampus})</span>` : "可";
                
                if(window.lastEventId !== args.eventId) { currentWeek = 0; window.lastEventId = args.eventId; }
                window.renderWeek();
                
                if(args.isClosed) { palette.style.display = 'none'; return; } 
                else { palette.style.display = 'flex'; }
                
                setTimeout(() => { window.setPen(selectedMode); }, 50);
                
                const g = document.getElementById('g'); if(!g) return;
                let down = false; let pressTimer = null; let isLongPress = false; let startX = 0, startY = 0; let touchMode = null;

                const handleStart = (e, x, y) => {
                    if (selectedMode === -1) return;
                    const cell = e.target.closest('.c'); if(!cell) return;
                    down = true; isLongPress = false; touchMode = null; startX = x; startY = y;
                    
                    pressTimer = setTimeout(() => {
                        if (touchMode !== 'scroll' && down) {
                            isLongPress = true; down = false; document.querySelectorAll('.pressing').forEach(el => el.classList.remove('pressing')); openModal(cell);
                        }
                    }, 400);
                };

                const handleMove = (e, x, y) => {
                    if (selectedMode === -1 || !down) return;
                    if (e.cancelable) e.preventDefault(); 
                    const cell = document.elementFromPoint(x, y)?.closest('.c');
                    if(cell) window.upd(cell, selectedMode);
                };

                const handleEnd = () => {
                    if (pressTimer) clearTimeout(pressTimer);
                    document.querySelectorAll('.pressing').forEach(el => el.classList.remove('pressing'));
                    if (down && touchMode === null && !isLongPress && selectedMode !== -1) {
                        const cell = document.elementFromPoint(startX, startY)?.closest('.c'); if(cell) window.upd(cell, selectedMode);
                    }
                    down = false; touchMode = null;
                };

                g.onmousedown = e => { handleStart(e, e.clientX, e.clientY); if(selectedMode !== -1) window.upd(e.target.closest('.c'), selectedMode); };
                g.onmousemove = e => { if (selectedMode === -1 || !down) return; const cell = document.elementFromPoint(e.clientX, e.clientY)?.closest('.c'); if(cell) window.upd(cell, selectedMode); }
                window.onmouseup = handleEnd; window.onmouseleave = handleEnd; 

                g.addEventListener('touchstart', e => { if (e.touches.length > 1) return; handleStart(e, e.touches[0].clientX, e.touches[0].clientY); }, {passive: true});
                g.addEventListener('touchmove', e => { if (selectedMode === -1) return; if (e.touches.length >= 2) return; if(down) { if (e.cancelable) e.preventDefault(); handleMove(e, e.touches[0].clientX, e.touches[0].clientY); } }, {passive: false});
                g.addEventListener('touchend', handleEnd); g.addEventListener('touchcancel', handleEnd);
                
                const btn = document.getElementById("submit-btn");
                if(btn) { btn.onclick = () => { 
                    const res = Array.from({length: numRows}, (_, r) => Array.from({length: totalDays}, (_, c) => parseInt(document.querySelector(`[data-r="${r}"][data-c="${c}"]`).dataset.v))); 
                    const commentText = document.getElementById("comment-box").value; 
                    setComponentValue({ data: res, comment: commentText, cell_details: window.cellDetails, trigger_save: true, ts: Date.now() }); 
                    btn.innerText = "⏳ 保存処理中..."; btn.style.backgroundColor = "#ff7b7b"; btn.style.pointerEvents = "none"; palette.style.display = 'none'; 
                }; }
                document.querySelectorAll('.c').forEach(cell => { window.upd(cell, cell.dataset.v); });
            }
        }); init(); </script></body></html>
        """)
grid_editor = components.declare_component("grid_editor", path="custom_editor_v4")
def call_gas(action, payload=None, method="POST"):
    try:
        p = payload or {}
        p["action"] = action
        res = requests.post(GAS_URL, json=p)
        return res.json()
    except Exception as e: 
        return {"status": "error", "message": str(e)}

def idx_to_time(i): return f"{(i*15)//60:02d}:{(i*15)%60:02d}"
time_master = [idx_to_time(i) for i in range(96)]

def get_border_top(t_str, event_type="time"):
    if event_type == "timetable": return "1px solid #aaa"
    if t_str.endswith(":00"): return "2px solid #555"
    elif t_str.endswith(":30"): return "1px dashed #999"
    else: return "1px solid #f0f0f0"

def format_deadline_jp(date_str):
    if pd.isna(date_str) or not date_str or str(date_str).strip() == "" or str(date_str) == "None": 
        return "期限なし"
    try:
        clean_str = str(date_str).split(' (')[0] 
        dt = pd.to_datetime(clean_str, errors='coerce')
        if pd.isna(dt): return "期限なし"
        wday = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        return f"{dt.month}/{dt.day}({wday}) {dt.strftime('%H:%M')}"
    except:
        return str(date_str)

campus_legend_html = """
<div style="margin: 20px 0; padding: 15px; background: #fff; border-radius: 10px; border: 2px solid #4CAF50; box-shadow: 0 4px 10px rgba(0,0,0,0.08);">
    <strong style="display:block; margin-bottom:12px; color:#2e7d32; font-size:15px;">🎨 キャンパスごとの模様（色見本）</strong>
    <div style="font-size: 13px; color: #555; margin-bottom: 15px; line-height: 1.5;">
        左が<span style="color:#4CAF50; font-weight:bold;">「可(緑)」</span>または<span style="color:#FBC02D; font-weight:bold;">「未定(黄)」</span>、右が<span style="color:#888; font-weight:bold;">「授業等(灰)」</span>として登録された際の見え方です。
    </div>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; font-size: 14px; color: #333;">
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>なかもず</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; background-image:repeating-linear-gradient(45deg, rgba(255,255,255,0.3), rgba(255,255,255,0.3) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; background-image:repeating-linear-gradient(45deg, rgba(255,255,255,0.7), rgba(255,255,255,0.7) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>すぎもと</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; background-image:repeating-linear-gradient(-45deg, rgba(0,0,0,0.15), rgba(0,0,0,0.15) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; background-image:repeating-linear-gradient(-45deg, rgba(0,0,0,0.1), rgba(0,0,0,0.1) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>あべの</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; background-image:radial-gradient(circle, rgba(255,255,255,0.4) 3px, transparent 4px); background-size:10px 10px; border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; background-image:radial-gradient(circle, rgba(255,255,255,0.8) 3px, transparent 4px); background-size:10px 10px; border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>りんくう</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; background-image:repeating-linear-gradient(90deg, rgba(255,255,255,0.3), rgba(255,255,255,0.3) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; background-image:repeating-linear-gradient(90deg, rgba(255,255,255,0.7), rgba(255,255,255,0.7) 5px, transparent 5px, transparent 10px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>もりのみや</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#4CAF50; background-image:repeating-linear-gradient(45deg, rgba(255,255,255,0.3), rgba(255,255,255,0.3) 3px, transparent 3px, transparent 6px), repeating-linear-gradient(-45deg, rgba(255,255,255,0.3), rgba(255,255,255,0.3) 3px, transparent 3px, transparent 6px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
                <div style="width:20px;height:20px; background:#e0e0e0; background-image:repeating-linear-gradient(45deg, rgba(255,255,255,0.7), rgba(255,255,255,0.7) 3px, transparent 3px, transparent 6px), repeating-linear-gradient(-45deg, rgba(255,255,255,0.7), rgba(255,255,255,0.7) 3px, transparent 3px, transparent 6px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>移動・他</b>
        </div>
        <div style="display:flex; align-items:center; gap:8px; grid-column: 1 / -1; margin-top: 5px;">
            <div style="display:flex; gap:2px;">
                <div style="width:20px;height:20px; background:#fff; background-image:repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(0,0,0,0.08) 4px, rgba(0,0,0,0.08) 8px), repeating-linear-gradient(-45deg, transparent, transparent 4px, rgba(0,0,0,0.08) 4px, rgba(0,0,0,0.08) 8px); border-radius:4px; border:1px solid #ccc; box-sizing:border-box;"></div>
            </div>
            <b>バイト/サークル等</b> <span style="font-size:12px; color:#888;">(※「不可[白]」扱いになります)</span>
        </div>
    </div>
</div>
"""

def main():
    if "app_initialized" not in st.session_state:
        st.session_state.app_initialized = True

    if st.session_state.get("save_success_msg"):
        st.toast(st.session_state.save_success_msg, icon="✅")
        st.session_state.save_success_msg = None

    if "event" in st.query_params:
        st.session_state.jump_to_event = st.query_params["event"]
        st.query_params.clear()

    if "auth" not in st.session_state: st.session_state.auth = None
    
    current_year = datetime.now().year
    MASTER_G2 = [f"{year}年度" for year in range(current_year - 6, current_year + 2)]
    
    config_doc = db.collection("config").document("master").get()
    if config_doc.exists:
        config_data = config_doc.to_dict()
        MASTER_G1 = config_data.get("g1", ["なかもず", "もりのみや", "すぎもと", "あべの", "りんくう"])
        MASTER_G3 = config_data.get("g3", ["卒業生ネットワーク関係者"])
    else:
        MASTER_G1 = ["なかもず", "もりのみや", "すぎもと", "あべの", "りんくう"]
        MASTER_G3 = ["卒業生ネットワーク関係者"]

    def sort_groups(lst, master):
        return sorted(lst, key=lambda x: master.index(x) if x in master else 999)
    
    # ==========================================
    # 🔑 未ログイン画面（ログイン・新規登録・復旧）
    # ==========================================
    if not st.session_state.auth:
        _, col_login, _ = st.columns([1, 2, 1])
        with col_login:
            st.title("V-Sync by もっきゅー")
            
            # 💡 ラジオボタンの項目名を統一（末尾のスペースを削除）
            login_mode = st.radio("メニュー", ["🔑 ログイン", "📝 新規アカウント作成", "🆘 PIN・パスワード復旧"], horizontal=True)
            st.markdown("---")
            
            if login_mode == "🔑 ログイン":
                with st.form("login_form"):
                    st.subheader("ログイン")
                    n = st.text_input("氏名", autocomplete="username")
                    p = st.text_input("PIN", type="password", autocomplete="current-password")
                    
                    if st.form_submit_button("ログイン", use_container_width=True, type="primary"):
                        hashed_p = hash_secret(p)
                        docs = db.collection("users").where("name", "==", n).stream()
                        user_doc = None
                        for doc in docs:
                            u_data = doc.to_dict()
                            # ハッシュ値または平文でのログイン成功判定
                            if u_data.get("pin") == hashed_p or u_data.get("pin") == p:
                                if u_data.get("pin") == p: # 平文ならハッシュ化して更新
                                    u_data["pin"] = hashed_p
                                    db.collection("users").document(str(u_data["user_id"])).update({"pin": hashed_p})
                                user_doc = u_data
                                break
                        if user_doc:
                            st.session_state.auth = user_doc
                            st.rerun()
                        else:
                            st.error("認証失敗: 氏名またはPINが間違っています")

            elif login_mode == "📝 新規アカウント作成":
                st.subheader("新規アカウント作成")
                st.info("💡 未所属の方でも、そのまま下部の登録ボタンを押して利用可能です。")
                reg_n = st.text_input("氏名 (スペースは自動で削除されます)", key="reg_name")
                reg_p = st.text_input("PIN (自由な文字列・数字)", type="password", key="reg_pin")
                reg_s = st.text_input("🔑 秘密の合言葉", key="reg_secret")
                
                st.markdown("---")
                g1 = st.multiselect("🏫 キャンパス", MASTER_G1, key="reg_g1")
                g2 = st.multiselect("🎓 入学年度", MASTER_G2, key="reg_g2")
                g3 = st.multiselect("🤝 オプション", MASTER_G3, key="reg_g3")

                if st.button("✅ 登録してログイン", use_container_width=True, type="primary"):
                    clean_name = reg_n.replace(" ", "").replace("　", "")
                    
                    # 💡 条件分岐はここから始まります
                    if clean_name and reg_p and reg_s:
                        all_users_list = [doc.to_dict() for doc in db.collection("users").stream()]
                        new_num = len(all_users_list) + 1
                        new_user_id = f"U{new_num:03}"

                        new_u = {
                            "user_id": new_user_id,
                            "name": clean_name,
                            "pin": hash_secret(reg_p),
                            "secret_word": hash_secret(reg_s),
                            "group_1": ", ".join(g1),
                            "group_2": ", ".join(g2),
                            "group_3": ", ".join(g3),
                            "group_4": "",
                            "role": "user"
                        }
                        
                        db.collection("users").document(new_user_id).set(new_u)
                        
                        gas_payload = new_u.copy()
                        gas_payload["pin"] = "ENCRYPTED_PIN"
                        gas_payload["secret_word"] = "SET_BY_USER"
                        backup_to_gas_async("register_user_v2", {"payload": gas_payload})
                        
                        st.session_state.auth = new_u
                        st.rerun()
                    else:
                        st.warning("氏名、PIN、秘密の合言葉はすべて必須です。")

            elif login_mode == "🆘 PIN・パスワード復旧":
                st.subheader("PINの再設定")
                with st.expander("🔑 秘密の合言葉を使って自分で復旧する", expanded=True):
                    with st.form("recovery_auth_form"):
                        st.markdown("<small>登録時に設定した合言葉がわかる方はこちら</small>", unsafe_allow_html=True)
                        rec_n = st.text_input("氏名")
                        rec_s = st.text_input("秘密の合言葉", type="password")
                        new_p = st.text_input("設定したい新しいPIN", type="password")
                        if st.form_submit_button("新しいPINで更新する", use_container_width=True, type="primary"):
                            clean_n = rec_n.replace(" ","").replace("　","")
                            docs = db.collection("users").where("name", "==", clean_n).stream()
                            target_user = None
                            for doc in docs: target_user = doc.to_dict(); break
                            
                            if target_user and target_user.get("secret_word") == hash_secret(rec_s):
                                db.collection("users").document(str(target_user["user_id"])).update({"pin": hash_secret(new_p)})
                                backup_to_gas_async("recover_account_v2", {"payload": {"name": clean_n, "new_pin": "ENCRYPTED_PIN"}})
                                st.success("✅ 更新成功！新しいPINでログインできます。")
                            else:
                                st.error("氏名または合言葉が間違っています。")
                
                with st.expander("🆘 合言葉も忘れたので、管理者に依頼する"):
                    st.write("管理者のチャットツール等へ通知を送り、PINのリセットを依頼します。")
                    req_name = st.text_input("あなたのお名前", key="req_pin_name")
                    if st.button("🚀 管理者にリセット依頼を送る", use_container_width=True):
                        if not req_name: 
                            st.warning("名前を入力してください。")
                        else:
                            backup_to_gas_async("request_pin_reset", {"payload": {"name": req_name}})
                            st.success(f"✅ {req_name}さん、管理者に通知を送りました。")
        return

    # ==========================================
    # ログイン後のメイン画面構築
    # ==========================================
    user = st.session_state.auth
    
    default_menu_index = 0
    if "jump_to_event" in st.session_state:
        st.session_state.target_ev_id = st.session_state.jump_to_event
        default_menu_index = 0
        del st.session_state.jump_to_event
    
    menu_opts = ["📅 日程調整 回答", "👤 プロフィール設定", "⏰ 時間割設定"]
    if user.get("role") in ["user", "admin", "top_admin"]: menu_opts.append("➕ イベント新規作成")
    if user.get("role") in ["admin", "top_admin"]: menu_opts.append("⚙️ 管理者専用")
    
    view_mode = st.sidebar.radio("🔧 メニュー", menu_opts, index=default_menu_index)

    # ----------------------------------------------------
    # 👤 プロフィール設定画面
    # ----------------------------------------------------
    if view_mode == "👤 プロフィール設定":
        st.title("👤 プロフィール設定")
        st.write("所属情報の更新を行います。（未所属にする場合は選択を解除してください）")
        
        def_g1 = [x for x in str(user.get('group_1', '')).split(', ') if x]
        def_g2 = [x for x in str(user.get('group_2', '')).split(', ') if x]
        def_g3 = [x for x in str(user.get('group_3', '')).split(', ') if x]

        safe_def_g1 = [x for x in def_g1 if x in MASTER_G1]
        safe_def_g2 = [x for x in def_g2 if x in MASTER_G2]
        safe_def_g3 = [x for x in def_g3 if x in MASTER_G3]

        upd_g1 = st.multiselect("🏫 現在通っているキャンパス", MASTER_G1, default=safe_def_g1, key="upd_g1")
        upd_g2 = st.multiselect("🎓 入学年度", MASTER_G2, default=safe_def_g2, key="upd_g2")
        upd_g3 = st.multiselect("🤝 オプション", MASTER_G3, default=safe_def_g3, key="upd_g3")

        st.markdown("---")
        st.markdown("##### 📅 外部カレンダー連携 (任意)")
        st.write("GoogleカレンダーやiPhoneの「非公開URL（iCal形式 / .ics）」を設定しておくと、日程調整の際に自分の予定を自動でグレーアウトできます。")
        upd_cal_url = st.text_input("カレンダーの非公開URL", value=user.get('calendar_url', ''), placeholder="https://calendar.google.com/calendar/ical/.../basic.ics")

        if st.button("💾 更新", type="primary"):
            payload = {"user_id": user['user_id'], "group_1": ", ".join(upd_g1), "group_2": ", ".join(upd_g2), "group_3": ", ".join(upd_g3), "calendar_url": upd_cal_url}
            
            db.collection("users").document(str(user["user_id"])).update(payload)
            
            gas_payload = payload.copy()
            if gas_payload.get("calendar_url"): 
                gas_payload["calendar_url"] = "LINKED"
            
            backup_to_gas_async("update_user_v2", {"payload": gas_payload})
            
            user.update(payload)
            st.session_state.auth = user
            st.success("✅ 保存完了")
            time.sleep(1)
            st.rerun()

        st.markdown("---")
        st.markdown("##### ⚠️ アカウントの削除（退会）")
        with st.expander("退会手続きを開く"):
            st.warning("退会すると、これまでの回答データや時間割がすべて削除され、元に戻すことはできません。")
            if st.button("💥 本当に退会する", type="primary"):
                uid = str(user["user_id"])
                # 1. ユーザー削除
                db.collection("users").document(uid).delete()
                # 2. 回答データの削除
                res_docs = db.collection("responses").where("user_id", "==", uid).stream()
                for d in res_docs:
                    db.collection("responses").document(d.id).delete()
                
                backup_to_gas_async("delete_user", {"payload": {"user_id": uid}})
                st.session_state.auth = None
                st.rerun()
        return

    # ----------------------------------------------------
    # ⏰ 時間割設定画面
    # ----------------------------------------------------
    if view_mode == "⏰ 時間割設定":
        st.title("⏰ 時間割設定")
        st.info("※ここで設定した授業・バイトの予定は、各イベントの日程調整画面で「時間割パワー反映」ボタンを押すことで、不可(×)や授業等(グレー)として一括で自動入力できます。")
        
        st.markdown("""
        <style>
            .mobile-rotate-guide { display: none; }
            @media (max-width: 650px) and (orientation: portrait) {
                .mobile-rotate-guide {
                    display: flex; align-items: center; justify-content: center;
                    background: linear-gradient(135deg, #e8f5e9, #c8e6c9); color: #2e7d32;
                    padding: 12px 15px; border-radius: 8px; margin-bottom: 20px;
                    font-size: 13px; font-weight: bold; border-left: 5px solid #4CAF50;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.05); animation: fadeIn 0.5s ease-in-out;
                }
                .mobile-rotate-guide::before { content: "📱🔄"; font-size: 20px; margin-right: 10px; }
            }
            @media (max-width: 650px) {
                [data-testid="stForm"] > div > div > [data-testid="stVerticalBlock"] {
                    overflow-x: auto !important; padding-bottom: 15px !important;
                }
                [data-testid="stHorizontalBlock"] {
                    display: flex !important; flex-direction: row !important; flex-wrap: nowrap !important;
                    min-width: 480px !important; gap: 2px !important;
                }
                [data-testid="column"] { min-width: 0 !important; flex: 1 1 0px !important; padding: 0 !important; }
                [data-testid="column"]:first-child { flex: 0 0 55px !important; }
                .tt-day-header { font-size: 13px !important; padding: 4px 0 !important; }
                .tt-time-cell { font-size: 11px !important; padding: 4px 2px !important; border-left: 2px solid #4CAF50 !important; }
                .tt-time-sub { font-size: 9px !important; display: block; line-height: 1.1; }
                .status-on, .status-off, .af-status-on { font-size: 10px !important; padding: 2px 0 !important; }
                [data-testid="stSelectbox"] { min-width: 0 !important; }
            }
        </style>
        <div class="mobile-rotate-guide">スマホを横向きにすると、時間割が綺麗に表示されます！</div>
        """, unsafe_allow_html=True)
        
        fixed_sched = user.get("fixed_schedule", {})
        try: fixed_locs = json.loads(user.get("group_4", "{}"))
        except: fixed_locs = {}

        ui_state = {str(i): {} for i in range(5)}
        days_jp = ["月", "火", "水", "木", "金"]
        col_ratios = [1.1, 1, 1, 1, 1, 1]
        
        tt_options = ["- (空き)"] + MASTER_G1 + ["その他/移動中", "💼 バイト/サークル等"]

        cols = st.columns(col_ratios)
        cols[0].markdown("<div style='padding:8px;'></div>", unsafe_allow_html=True)
        for i, d in enumerate(days_jp): cols[i+1].markdown(f"<div class='tt-day-header'>{d}</div>", unsafe_allow_html=True)
        st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)

        periods = [
            ("1限", "09:00〜", 36, 42, "p1"), ("2限", "10:45〜", 43, 49, "p2"),
            ("3限", "13:15〜", 53, 59, "p3"), ("4限", "15:00〜", 60, 66, "p4"), ("5限", "16:45〜", 67, 73, "p5")
        ]
        
        for p_name, p_time, s_idx, e_idx, p_key in periods:
            cols = st.columns(col_ratios)
            cols[0].markdown(f"<div class='tt-time-cell'>{p_name}<br><span class='tt-time-sub'>{p_time}</span></div>", unsafe_allow_html=True)
            for i in range(5):
                day_bin = fixed_sched.get(str(i), "0"*96)
                is_occupied = (day_bin[s_idx:e_idx] == "1" * (e_idx - s_idx))
                saved_loc = fixed_locs.get(str(i), {}).get(p_key, "")
                
                if not is_occupied: current_val = "- (空き)"
                elif saved_loc in tt_options: current_val = saved_loc
                elif "バイト" in saved_loc or "私用" in saved_loc: current_val = "💼 バイト/サークル等"
                else: current_val = tt_options[1]
                
                selected_opt = cols[i+1].selectbox("予定", tt_options, index=tt_options.index(current_val), key=f"tt_{p_key}_{i}", label_visibility="collapsed")
                
                if selected_opt == "- (空き)":
                    ui_state[str(i)][p_key] = False
                    cols[i+1].markdown("<div class='status-off'>-</div>", unsafe_allow_html=True)
                elif selected_opt == "💼 バイト/サークル等":
                    ui_state[str(i)][p_key] = True
                    ui_state[str(i)][f"{p_key}_loc"] = "💼 バイト/サークル等"
                    cols[i+1].markdown(f"<div class='status-off' style='background:#f5f5f5; color:#333; font-size:10px; padding:2px 0; border:none;'>💼 バイト等</div>", unsafe_allow_html=True)
                else:
                    ui_state[str(i)][p_key] = True
                    ui_state[str(i)][f"{p_key}_loc"] = selected_opt
                    cols[i+1].markdown(f"<div class='status-on' style='font-size:10px; padding:2px 0;'>✔︎ {selected_opt}</div>", unsafe_allow_html=True)
            st.markdown("<hr style='margin: 4px 0; border: none; border-bottom: 1px dashed #ddd;'>", unsafe_allow_html=True)
            
        cols = st.columns(col_ratios)
        cols[0].markdown(f"<div class='tt-time-cell' style='border-left-color:#FF9800;'>放課後<br><span class='tt-time-sub'>18:30〜</span></div>", unsafe_allow_html=True)
        for i in range(5):
            day_bin = fixed_sched.get(str(i), "0"*96); af_bin = day_bin[74:]
            is_occupied = "1" in af_bin
            saved_loc = fixed_locs.get(str(i), {}).get("af", "")
            
            if not is_occupied: current_val = "- (空き)"
            elif saved_loc in tt_options: current_val = saved_loc
            elif "バイト" in saved_loc or "私用" in saved_loc: current_val = "💼 バイト/サークル等"
            else: current_val = tt_options[1]
            
            selected_opt = cols[i+1].selectbox("予定", tt_options, index=tt_options.index(current_val), key=f"tt_af_{i}", label_visibility="collapsed")
            
            if selected_opt == "- (空き)":
                ui_state[str(i)]["af"] = False
                cols[i+1].markdown("<div class='status-off'>-</div>", unsafe_allow_html=True)
            elif selected_opt == "💼 バイト/サークル等":
                ui_state[str(i)]["af"] = True
                ui_state[str(i)]["af_loc"] = "💼 バイト/サークル等"
                cols[i+1].markdown(f"<div class='status-off' style='background:#f5f5f5; color:#333; font-size:10px; padding:2px 0; border:none;'>💼 バイト等</div>", unsafe_allow_html=True)
            else:
                ui_state[str(i)]["af"] = True
                ui_state[str(i)]["af_loc"] = selected_opt
                cols[i+1].markdown(f"<div class='af-status-on' style='font-size:10px; padding:2px 0;'>🌙 {selected_opt}</div>", unsafe_allow_html=True)
        st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)
        
        cols = st.columns(col_ratios)
        cols[0].markdown(f"<div class='tt-end-time' style='text-align:center; font-size:10px; color:#666; padding-top:10px;'>終了時刻</div>", unsafe_allow_html=True)
        for i in range(5):
            if ui_state[str(i)]["af"]:
                day_bin = fixed_sched.get(str(i), "0"*96); af_bin = day_bin[74:]; af_end_val = "21:00"
                if "1" in af_bin:
                    last_idx = 74 + af_bin.rfind("1")
                    if last_idx + 1 < len(time_master): af_end_val = time_master[last_idx + 1]
                    else: af_end_val = "23:45"
                af_opts = [idx_to_time(idx) for idx in range(76, 96)]
                af_idx = af_opts.index(af_end_val) if af_end_val in af_opts else 8
                ui_state[str(i)]["af_end"] = cols[i+1].selectbox("終了時間", af_opts, index=af_idx, key=f"afe_{i}", label_visibility="collapsed")
            else:
                cols[i+1].markdown("<div style='text-align:center; color:#ccc; padding-top:10px; font-size:12px;'>-</div>", unsafe_allow_html=True)
                ui_state[str(i)]["af_end"] = "21:00"

        st.markdown("<br><br>", unsafe_allow_html=True)
        if st.button("💾 時間割を保存する", use_container_width=True, type="primary"):
            new_fixed_sched = {}
            new_fixed_locs = {}
            for i in range(5):
                wd_str = str(i); new_bin = ["0"] * 96; day_locs = {}
                
                if ui_state[wd_str]["p1"]: 
                    new_bin[36:42] = ["1"] * 6; day_locs["p1"] = ui_state[wd_str]["p1_loc"]
                if ui_state[wd_str]["p2"]: 
                    new_bin[43:49] = ["1"] * 6; day_locs["p2"] = ui_state[wd_str]["p2_loc"]
                if ui_state[wd_str]["p3"]: 
                    new_bin[53:59] = ["1"] * 6; day_locs["p3"] = ui_state[wd_str]["p3_loc"]
                if ui_state[wd_str]["p4"]: 
                    new_bin[60:66] = ["1"] * 6; day_locs["p4"] = ui_state[wd_str]["p4_loc"]
                if ui_state[wd_str]["p5"]: 
                    new_bin[67:73] = ["1"] * 6; day_locs["p5"] = ui_state[wd_str]["p5_loc"]
                if ui_state[wd_str]["af"]:
                    end_idx = time_master.index(ui_state[wd_str]["af_end"])
                    new_bin[74:end_idx] = ["1"] * (end_idx - 74)
                    day_locs["af"] = ui_state[wd_str]["af_loc"]
                    
                new_fixed_sched[wd_str] = "".join(new_bin)
                new_fixed_locs[wd_str] = day_locs
                
            payload = {"user_id": user['user_id'], "fixed_schedule": new_fixed_sched, "group_4": json.dumps(new_fixed_locs)}
            res = call_gas("update_user", {"payload": payload}, method="POST")
            if res.get("status") == "success":
                updated_u = res.get("data")
                db.collection("users").document(str(updated_u["user_id"])).update(updated_u)
                st.session_state.auth = updated_u
                st.rerun()
            else:
                st.error("更新に失敗しました。")
        return

    # ----------------------------------------------------
    # ➕ イベント新規作成画面
    # ----------------------------------------------------
    if view_mode == "➕ イベント新規作成":
        st.title("➕ イベント新規作成")
        st.write("新しい日程調整イベントを作成します。")
        
        ev_type_label = st.radio("📝 日程調整のタイプを選択", ["🕒 時間帯 (15分刻み)", "🏫 時間割 (月〜金)", "📅 複数の予定 (候補から選択)"], horizontal=True)
        st.markdown("<br>", unsafe_allow_html=True)
        
        ev_title = st.text_input("イベント名")
        st.markdown("<br>", unsafe_allow_html=True)
        
        with st.container(border=True):
            st.markdown("##### ⏳ 回答期限の設定")
            col_d1, col_d2 = st.columns([1, 1])
            with col_d1: deadline_date = st.date_input("回答期限 (日付)", value=datetime.today() + timedelta(days=7))
            with col_d2: deadline_time = st.time_input("回答期限 (時刻)", value=datetime.strptime("23:59", "%H:%M").time())
            auto_close = st.checkbox("✅ 期限が過ぎたら自動で締め切る (回答不可にする)", value=True)
            
        ev_start, ev_end = None, None
        t_start, t_end = None, None
        opts_list = []
        
        if ev_type_label == "🕒 時間帯 (15分刻み)":
            ev_type = "time"
            with st.container(border=True):
                st.markdown("##### 📅 1. カレンダーに表示する【期間】")
                col1, col_m1, col2 = st.columns([10, 1, 10])
                with col1: ev_start = st.date_input("開始日", label_visibility="collapsed")
                with col_m1: st.markdown("<div style='text-align:center; font-weight:bold; font-size:18px;'>〜</div>", unsafe_allow_html=True)
                with col2: ev_end = st.date_input("終了日", label_visibility="collapsed")
                
            with st.container(border=True):
                st.markdown("##### ⏰ 2. 1日あたりの対象【時間帯】")
                col3, col_m2, col4 = st.columns([10, 1, 10])
                with col3: t_start = st.selectbox("開始時刻", time_master, index=36, label_visibility="collapsed")
                with col_m2: st.markdown("<div style='text-align:center; font-weight:bold; font-size:18px;'>〜</div>", unsafe_allow_html=True)
                with col4: t_end = st.selectbox("終了時刻", time_master, index=72, label_visibility="collapsed")
                
        elif ev_type_label == "🏫 時間割 (月〜金)":
            ev_type = "timetable"
            st.info("💡 月曜〜金曜の「1限〜5限・放課後」の枠のみを使って、全員の空きコマを一括で集計するモードです。")
            
        else:
            ev_type = "options"
            st.info("💡 任意の予定（候補日やイベント案など）をリスト化し、どれに参加できるかアンケートを取るモードです。")
            if "opt_count" not in st.session_state: st.session_state.opt_count = 3
            with st.container(border=True):
                st.markdown("##### 📅 候補リストを作成")
                for i in range(st.session_state.opt_count):
                    val = st.text_input(f"候補 {i+1}", key=f"new_opt_{i}", placeholder="例: 4/1 19:00〜 新歓")
                    opts_list.append(val)
                if st.button("➕ 候補を追加する", use_container_width=True):
                    st.session_state.opt_count += 1
                    st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown("##### 👥 参加メンバーの指定")
            is_all_members = st.checkbox("✅ 全員に公開する（デフォルト）", value=True, key="create_all_members")
            target_scope_json = ""
            
            if not is_all_members:
                all_u = [d.to_dict() for d in db.collection("users").stream()]
                
                all_g1 = sort_groups(list(set([g.strip() for u in all_u for g in str(u.get('group_1', '')).split(',') if g.strip()])), MASTER_G1)
                all_g2 = sort_groups(list(set([g.strip() for u in all_u for g in str(u.get('group_2', '')).split(',') if g.strip()])), MASTER_G2)
                all_g3 = sort_groups(list(set([g.strip() for u in all_u for g in str(u.get('group_3', '')).split(',') if g.strip()])), MASTER_G3)
                
                st.markdown("<span style='font-size:13px; color:#555;'>※ここで指定したグループや個人のみにイベントが表示されます。</span>", unsafe_allow_html=True)
                col_t1, col_t2 = st.columns(2)
                with col_t1:
                    t_g1 = st.multiselect("🏫 キャンパス", all_g1, key="tgt_g1")
                    t_g3 = st.multiselect("🤝 オプション", all_g3, key="tgt_g3")
                with col_t2:
                    t_g2 = st.multiselect("🎓 入学年度", all_g2, key="tgt_g2")
                    t_users = st.multiselect("👤 特定の個人", sorted(all_u, key=lambda x: x.get('name', '')), format_func=lambda x: f"{x.get('name')} (ID: {x.get('user_id')})", key="tgt_users")
                
                target_scope_json = json.dumps({
                    "groups": t_g1 + t_g2 + t_g3,
                    "users": [u['user_id'] for u in t_users]
                })
                
                # 💡【追加】メンションのプレビューと警告UI
                mentions_preview = []
                for g in t_g1: mentions_preview.append(f"@{g}")
                for g in t_g2: mentions_preview.append(f"@{g.replace('年度', '年度入学生')}")
                for g in t_g3: mentions_preview.append(f"@{g}")
                mentions_preview = list(dict.fromkeys(mentions_preview))
                
                if mentions_preview:
                    st.info(f"📣 **Discord通知プレビュー:**\n `{' '.join(mentions_preview)}` に通知が飛びます")
                    if len(mentions_preview) >= 4:
                        st.warning("⚠️ **通知範囲が広すぎませんか？**\n多数のグループを選択しています。本当に必要な人だけか確認してください。")

        st.markdown("<br>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown("##### 🔒 プライバシー・通知設定")
            is_private = st.checkbox("🤫 プライベート調整にする（回答者の名前を他の参加者に隠す）", key="create_private")
            skip_discord = st.checkbox("🔕 Discordに通知を送らない（ひっそり作成してURLで直接招待する）", value=False, key="skip_discord")

        st.markdown("##### 📝 イベントの説明・備考")
        ev_desc_raw = rt_editor(key="desc_editor")
        ev_desc = ev_desc_raw if ev_desc_raw else ""
        
        if st.button("🚀 イベントを作成", use_container_width=True, type="primary"):
            if not ev_title: st.warning("イベント名を入力してください。")
            elif ev_type == "time" and ev_start > ev_end: st.error("終了日は開始日以降に設定してください。")
            elif ev_type == "time" and time_master.index(t_start) >= time_master.index(t_end): st.error("終了時刻は開始時刻より後に設定してください。")
            elif ev_type == "options" and not any(o.strip() for o in opts_list): st.error("最低1つの候補を入力してください。")
            elif not is_all_members and target_scope_json == '{"groups": [], "users": []}': st.error("対象メンバーを指定するか、「全員に公開する」にチェックを入れてください。")
            else:
                if is_all_members:
                    mention_text = "@everyone"
                else:
                    mention_text = " ".join(mentions_preview) if not is_all_members else "@everyone"

                deadline_str = f"{deadline_date.strftime('%Y-%m-%d')} {deadline_time.strftime('%H:%M')}"
                
                # 💡 Python側でイベントIDを即時発行
                created_event_id = generate_custom_id("EV")
                
                payload = {
                    "event_id": created_event_id,
                    "title": ev_title, 
                    "description": ev_desc, 
                    "start_date": ev_start.strftime("%Y-%m-%d") if ev_type == "time" else "", 
                    "end_date": ev_end.strftime("%Y-%m-%d") if ev_type == "time" else "", 
                    "start_time_idx": time_master.index(t_start) if ev_type == "time" else 0, 
                    "end_time_idx": time_master.index(t_end) if ev_type == "time" else 0, 
                    "status": "open",
                    "type": ev_type,
                    "options_name": json.dumps([o.strip() for o in opts_list if o.strip()]) if ev_type == "options" else "",
                    "close_time": deadline_str,
                    "auto_close": auto_close,
                    "target_scope": target_scope_json,
                    "is_private": is_private,
                    "skip_discord": skip_discord,
                    "mention_text": mention_text
                }
                
                # 1. Firestoreに保存
                db.collection("events").document(created_event_id).set(payload)
                
                # 2. 修正：辞書で包んでGASへ送る
                backup_to_gas_async("create_event_v2", {"payload": payload})
                
                st.success(f"「{ev_title}」を作成しました！")
                
                share_url = f"{APP_BASE_URL}?event={created_event_id}"
                st.info("👇 以下の招待リンクをコピーして、参加者に送ってください（右上のアイコンでコピーできます）")
                st.code(share_url, language="text")
                
                if "opt_count" in st.session_state: del st.session_state.opt_count
        return

    # ----------------------------------------------------
    # ⚙️ 管理者専用画面
    # ----------------------------------------------------
    if view_mode == "⚙️ 管理者専用":
        st.title("⚙️ 管理者ダッシュボード")
        tab_manage, tab_users = st.tabs(["📝 イベント・アーカイブ管理", "👥 ユーザー管理"])

        with tab_manage:
            all_users_admin = [d.to_dict() for d in db.collection("users").stream()]
            user_map = {str(u.get('user_id')): u.get('name') for u in all_users_admin}

            def format_target_scope(scope_str):
                if not scope_str or not scope_str.startswith('{'): return "全員"
                try:
                    scope = json.loads(scope_str)
                    groups = scope.get("groups", []); users = scope.get("users", [])
                    user_names = [user_map.get(str(uid), str(uid)) for uid in users]
                    res = []
                    if groups: res.append(f"📁 {', '.join(groups)}")
                    if user_names: res.append(f"👤 {', '.join(user_names)}")
                    return " / ".join(res) if res else "全員"
                except: return "限定"

            all_events = [d.to_dict() for d in db.collection("events").stream()]
            
            if all_events:
                for ev in all_events:
                    ev['type_safe'] = ev.get('type') or ev.get('event_type', 'time')
                    ev['start_idx_safe'] = ev.get('start_time_idx') or ev.get('start_idx', 0)
                    ev['end_idx_safe'] = ev.get('end_time_idx') or ev.get('end_idx', 0)
                    ev['deadline_safe'] = ev.get('close_time') or ev.get('deadline', '')
                    
                df_ev = pd.DataFrame(all_events)
                df_ev['種類'] = df_ev['type_safe'].replace({"time": "🕒 時間", "timetable": "🏫 時間割", "options": "📅 予定候補"})
                
                # 💡 「詳細」の表示を直感的に（終了時刻はそのままで「〜18:00」になる）
                df_ev['詳細'] = df_ev.apply(lambda row: f"{idx_to_time(row.get('start_idx_safe', 0))}〜{idx_to_time(row.get('end_idx_safe', 0))}" if row.get('type_safe')=='time' else ("月〜金" if row.get('type_safe')=='timetable' else "複数候補"), axis=1)
                
                df_ev['期限'] = df_ev['deadline_safe'].apply(format_deadline_jp)
                df_ev['公開範囲'] = df_ev['target_scope'].apply(format_target_scope)
                df_ev['秘密'] = df_ev['is_private'].apply(lambda x: "🤫" if x else "-")
                df_ev['招待URL'] = df_ev['event_id'].apply(lambda x: f"{APP_BASE_URL}?event={x}")
                
                df_display = df_ev[['event_id', 'title', '種類', '詳細', '期限', '公開範囲', '秘密', '招待URL', 'status']]
                
                active_events = [ev for ev in all_events if ev.get('status') in ['open', 'closed']]
                st.subheader("🟢 現在のイベント")
                html_table_ev = df_display[df_display['status'].isin(['open', 'closed'])].to_html(index=False, border=0, classes="custom-tbl")
                st.markdown("<style>.custom-tbl { width: 100%; border-collapse: collapse; font-size: 14px; text-align: left; } .custom-tbl th { background-color: #f0f2f6; padding: 10px; border-bottom: 2px solid #4CAF50; white-space: nowrap; } .custom-tbl td { padding: 10px; border-bottom: 1px solid #eee; word-break: break-all; }</style>" + f'<div style="overflow-x: auto; border: 1px solid #e0e0e0; border-radius: 8px;">{html_table_ev}</div>', unsafe_allow_html=True)
                
                st.markdown("---")
                st.subheader("⚙️ ステータス手動変更")
                if active_events:
                    with st.form("update_status_form"):
                        target_ev = st.selectbox("対象イベント", active_events, format_func=lambda x: f"{x['title']} ({x['status']})")
                        new_status = st.selectbox("ステータス", ["open", "closed", "archived"], index=1)
                        if st.form_submit_button("更新する"):
                            call_gas("update_event_status", {"payload": {"event_id": target_ev['event_id'], "status": new_status}}, method="POST")
                            db.collection("events").document(target_ev['event_id']).update({"status": new_status})
                            st.rerun()
                            
                st.markdown("---")
                st.subheader("👀 未回答者の抽出")
                if active_events:
                    check_ev = st.selectbox("確認するイベントを選択", active_events, format_func=lambda x: f"{x['title']} ({x['status']})", key="chk_unanswered")
                    if st.button("未回答者を抽出する", type="primary"):
                        with st.spinner("データを照合中..."):
                            ans_docs = db.collection("responses").where("event_id", "==", check_ev['event_id']).stream()
                            answered_uids = [str(d.to_dict().get("user_id")) for d in ans_docs]

                            target_users = []
                            scope_str = check_ev.get('target_scope', '')
                            is_all = True
                            t_groups, t_uids = [], []
                            if scope_str and scope_str.startswith('{'):
                                try:
                                    scope = json.loads(scope_str)
                                    t_groups = scope.get("groups", [])
                                    t_uids = [str(x) for x in scope.get("users", [])]
                                    if t_groups or t_uids: is_all = False
                                except: pass

                            for u in all_users_admin:
                                uid = str(u.get('user_id'))
                                if uid in answered_uids: continue

                                if is_all:
                                    target_users.append(u.get('name', ''))
                                else:
                                    if uid in t_uids:
                                        target_users.append(u.get('name', ''))
                                        continue
                                    u_g = []
                                    for g_key in ['group_1', 'group_2', 'group_3']:
                                        u_g.extend([x.strip() for x in str(u.get(g_key, '')).split(',') if x.strip()])
                                    
                                    if set(t_groups).intersection(set(u_g)):
                                        target_users.append(u.get('name', ''))

                            if target_users:
                                st.warning(f"⚠️ 対象者のうち、未回答の人が {len(target_users)} 名います。")
                                target_users = sorted(target_users)
                                st.code("\n".join(target_users), language="text")
                            else:
                                st.success("🎉 対象者は全員回答済みです！")

                st.markdown("---")
                st.subheader("📦 アーカイブ済み")
                html_table_arch = df_display[df_display['status'] == 'archived'].to_html(index=False, border=0, classes="custom-tbl")
                st.markdown(f'<div style="overflow-x: auto; border: 1px solid #e0e0e0; border-radius: 8px;">{html_table_arch}</div>', unsafe_allow_html=True)
            else: st.info("イベントがありません。")

        with tab_users:
            st.subheader("👥 ユーザー一覧と権限管理")
            
            all_users = [d.to_dict() for d in db.collection("users").stream()]
            
            if st.button("🔄 ユーザー一覧を最新に更新"):
                st.rerun()

            if all_users:
                df_u = pd.DataFrame(all_users)
                df_u = df_u.rename(columns={"user_id": "ユーザーID", "name": "氏名", "role": "権限", "group_1": "キャンパス", "group_2": "入学年度", "group_3": "オプション"})
                df_u["権限"] = df_u["権限"].replace({"top_admin": "👑 最高管理者", "admin": "🛠️ 管理者", "user": "📝 ユーザー", "guest": "👤 ゲスト"})
                display_cols = ["ユーザーID", "氏名", "権限", "キャンパス", "入学年度", "オプション"]
                display_cols = [c for c in display_cols if c in df_u.columns]
                
                html_table = df_u[display_cols].to_html(index=False, border=0, classes="custom-tbl")
                table_html = "<style>.custom-tbl { width: 100%; border-collapse: collapse; font-size: 14px; text-align: left; } .custom-tbl th { background-color: #f0f2f6; padding: 10px; border-bottom: 2px solid #4CAF50; white-space: nowrap; color: #333; } .custom-tbl td { padding: 10px; border-bottom: 1px solid #eee; color: #333; word-break: break-all; } .custom-tbl tr:hover { background-color: #f8f9fa; }</style>" + f'<div style="overflow-x: auto; border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 20px;">{html_table}</div>'
                st.markdown(table_html, unsafe_allow_html=True)
                
                st.markdown("---")
                st.subheader("🚨 ユーザー情報の更新 (PINリセット等)")
                tgt_user = st.selectbox("対象ユーザー", all_users, format_func=lambda x: f"{x.get('name')} (ID: {x.get('user_id')})")
                
                with st.form("admin_user_update"):
                    new_u_pin = st.text_input("新しいPIN (リセットする場合のみ入力)", type="password", autocomplete="new-password")
                    
                    if user.get("role") == "top_admin":
                        st.info("👑 最高管理者メニュー: ユーザーIDと氏名の変更が可能です。")
                        new_u_id = st.text_input("ユーザーID", value=tgt_user.get('user_id'))
                        new_u_name = st.text_input("氏名", value=tgt_user.get('name'))
                        
                        role_opts = ["guest", "user", "admin"]
                        if tgt_user.get('role') == 'top_admin':
                            st.info("※最高管理者の権限はここで変更できません。下の譲渡メニューを使用してください。")
                            new_u_role = "top_admin"
                        else:
                            current_role = tgt_user.get('role') if tgt_user.get('role') in role_opts else "guest"
                            new_u_role = st.selectbox("権限の変更", role_opts, index=role_opts.index(current_role))
                    else:
                        st.info("※権限（Role）やユーザーID・氏名の変更は top_admin のみ可能です。")
                        new_u_id = tgt_user.get('user_id')
                        new_u_name = tgt_user.get('name')
                        new_u_role = tgt_user.get('role')
                        
                    del_check = st.checkbox("💥 このユーザーを完全に削除する (復旧不可)")

                    if st.form_submit_button("更新/削除 実行", type="primary"):
                        if del_check:
                            if tgt_user.get('role') == 'top_admin':
                                st.error("最高管理者は削除できません。先に譲渡してください。")
                            else:
                                uid = str(tgt_user['user_id'])
                                db.collection("users").document(uid).delete()
                                res_docs = db.collection("responses").where("user_id", "==", uid).stream()
                                for d in res_docs:
                                    db.collection("responses").document(d.id).delete()
                                
                                backup_to_gas_async("delete_user", {"payload": {"user_id": uid}})
                                st.success(f"ユーザー {uid} を削除しました")
                                time.sleep(1.5)
                                st.rerun()
                        else:
                            if tgt_user.get('role') == 'top_admin' and new_u_role != 'top_admin':
                                st.error("最高管理者の権限は変更できません。")
                            else:
                                old_uid = str(tgt_user['user_id'])
                                new_uid = new_u_id.strip() if new_u_id else old_uid
                                new_name = new_u_name.strip() if new_u_name else tgt_user.get('name')
                                
                                updates = {"role": new_u_role, "name": new_name}
                                gas_payload = {"user_id": old_uid, "role": new_u_role, "name": new_name, "new_pin": ""}
                                
                                if new_uid != old_uid:
                                    updates["user_id"] = new_uid
                                    gas_payload["new_user_id"] = new_uid
                                    
                                if new_u_pin:
                                    updates["pin"] = hash_secret(new_u_pin)
                                    gas_payload["new_pin"] = "ENCRYPTED_PIN"
                                    
                                try:
                                    if new_uid != old_uid:
                                        existing = db.collection("users").document(new_uid).get()
                                        if existing.exists:
                                            st.error(f"エラー: ユーザーID '{new_uid}' は既に存在します。")
                                            st.stop()
                                            
                                        new_user_data = {**tgt_user, **updates}
                                        db.collection("users").document(new_uid).set(new_user_data)
                                        db.collection("users").document(old_uid).delete()
                                        
                                        # 回答データの移行
                                        res_docs = db.collection("responses").where("user_id", "==", old_uid).stream()
                                        for r_doc in res_docs:
                                            r_data = r_doc.to_dict()
                                            r_event_id = r_data.get("event_id")
                                            r_data["user_id"] = new_uid
                                            db.collection("responses").document(f"{r_event_id}_{new_uid}").set(r_data)
                                            db.collection("responses").document(r_doc.id).delete()
                                    else:
                                        db.collection("users").document(old_uid).update(updates)
                                        
                                    backup_to_gas_async("admin_update_user", {"payload": gas_payload})
                                    st.success("ユーザー情報を更新しました！")
                                    time.sleep(1.5)
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"更新中にエラーが発生しました: {e}")

                if user.get("role") == "top_admin":
                    st.markdown("---")
                    st.subheader("👑 最高管理者 (top_admin) の譲渡")
                    st.warning("⚠️ この操作を実行すると、あなたは `admin` に降格し、元に戻すことはできません。")
                    
                    candidates = [u for u in all_users if u.get('user_id') != user.get('user_id')]
                    new_top = st.selectbox("譲渡先ユーザー", candidates, format_func=lambda x: f"{x.get('name')} (ID: {x.get('user_id')})")
                    
                    st.markdown("<span style='font-size:13px; color:#555;'>PINリセットなどのSOSを受け取るための、新しい管理者の<b>DiscordユーザーID（18桁前後の数字）</b>を入力してください。<br>※Discordの設定から「開発者モード」をオンにし、プロフィールを右クリックしてIDをコピーできます。</span>", unsafe_allow_html=True)
                    new_discord_id = st.text_input("DiscordユーザーID (例: 123456789012345678)")
                    
                    if st.button("🔔 テスト通知を送信"):
                        if new_discord_id:
                            clean_id = new_discord_id.strip().replace("@", "").replace("<", "").replace(">", "")
                            mention_str = f"<@{clean_id}>"

                            res = call_gas("test_discord_mention", {"payload": {"discord_id": mention_str}}, method="POST")
                            if res.get("status") == "success":
                                st.success(f"Discordにテスト通知を送信しました！通知が来ているか確認してください。")
                            else:
                                st.error("テスト通知の送信に失敗しました。")
                        else:
                            st.warning("DiscordユーザーIDを入力してください。")
                    
                    confirm_transfer = st.checkbox("✅ Discordでテスト通知が届いたことを確認しました")
                    
                    if confirm_transfer:
                        if st.button("🚀 top_adminを譲渡する", type="primary"):
                            clean_id = new_discord_id.strip().replace("@", "").replace("<", "").replace(">", "")
                            mention_str = f"<@{clean_id}>"
                                
                            call_gas("transfer_top_admin", {"payload": {"caller_id": user['user_id'], "target_id": new_top['user_id'], "discord_id": mention_str}}, method="POST")
                            
                            db.collection("users").document(str(user['user_id'])).update({"role": "admin"})
                            updates_top = {"role": "top_admin"}
                            if mention_str: updates_top["discord_id"] = mention_str
                            db.collection("users").document(str(new_top['user_id'])).update(updates_top)
                            
                            st.session_state.auth = None
                            st.rerun()
        return

    # ----------------------------------------------------
    # 📅 一般ユーザー画面 (回答・集計)
    # ----------------------------------------------------
    active_groups = [str(user.get(f"group_{i}", "")).strip() for i in range(1, 4)]
    active_groups = [g for g in active_groups if g]
    group_str = f"<span style='color: #666; font-size: 0.9em; margin-left: 10px;'>({' / '.join(active_groups)})</span>" if active_groups else "<span style='color: #aaa; font-size: 0.9em; margin-left: 10px;'>(未所属)</span>"
    role_emoji = {"top_admin": "👑", "admin": "🛠️", "user": "📝", "guest": "👤"}.get(user.get("role"), "👤")
    st.markdown(f'<div class="user-header"><div style="font-size: 1.1em;"><b>{role_emoji} {user.get("name", "")}</b> さん {group_str}</div><div style="font-size: 0.8em; background: #e0e0e0; padding: 3px 8px; border-radius: 12px;">ID: {user.get("user_id", "")}</div></div>', unsafe_allow_html=True)

    current_ev_id = st.session_state.get("target_ev_id", "")
    
    # 🚀 [爆速化の要] 読み込み通信はここで1回だけ (0.1秒)
    all_users_fs, events, user_map_fs = get_app_data_from_firestore(user)
    st.session_state.event_responses = fetch_responses_for_event(current_ev_id, user_map_fs) if current_ev_id else []

    if not events: 
        st.info("現在表示できるイベントはありません。")
        return

    now_dt = datetime.now()

    unanswered_events = []
    for ev in events:
        if not ev.get('is_answered') and ev.get('status') == 'open':
            unanswered_events.append(ev)
            
    if unanswered_events:
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"<div style='color:#FF4B4B; font-weight:bold; padding-bottom: 5px;'>📢 未回答の予定 ({len(unanswered_events)}件)</div>", unsafe_allow_html=True)
        for u_ev in unanswered_events:
            is_urgent = False
            ev_close_time = u_ev.get('close_time') or u_ev.get('deadline', '')
            if ev_close_time:
                try:
                    dl_dt = pd.to_datetime(ev_close_time, errors='coerce')
                    if pd.notna(dl_dt):
                        dl_dt = dl_dt.tz_localize(None)
                        if 0 <= (dl_dt - now_dt).total_seconds() <= 3 * 24 * 3600:
                            is_urgent = True
                except: pass
            
            icon = "🔥" if is_urgent else "🔴"
            dl_text = format_deadline_jp(ev_close_time)
            
            if st.sidebar.button(f"{icon} {u_ev.get('title', '')} (〜{dl_text})", key=f"side_btn_{u_ev.get('event_id')}", use_container_width=True):
                st.session_state.target_ev_id = u_ev.get('event_id')
                st.rerun()

    if unanswered_events:
        st.warning(f"⚠️ **未回答のイベントが {len(unanswered_events)} 件あります！** サイドバーのリストから選択して早めの回答をお願いします。")

    default_idx = 0
    target_id = st.session_state.get("target_ev_id")
    
    if target_id:
        for i, ev in enumerate(events):
            if ev.get('event_id') == target_id:
                default_idx = i
                break
        else:
            st.session_state.target_ev_id = events[0].get('event_id')

    def format_ev_name(x):
        ev_close_time = x.get('close_time') or x.get('deadline', '')
        dl_str = format_deadline_jp(ev_close_time)
        if x.get('status') == 'closed': 
            icon = "🔒"
        elif x.get('is_answered'): 
            icon = "✅"
        else:
            is_urgent = False
            try:
                if ev_close_time:
                    dl_dt = pd.to_datetime(ev_close_time, errors='coerce')
                    if pd.notna(dl_dt):
                        dl_dt = dl_dt.tz_localize(None)
                        if 0 <= (dl_dt - now_dt).total_seconds() <= 3 * 24 * 3600:
                            is_urgent = True
            except: pass
            icon = "🔥" if is_urgent else "🔴"
            
        return f"{icon} {x.get('title', '')} [締切: {dl_str}]"

    event = st.selectbox("🎯 対象イベント選択", events, index=default_idx, format_func=format_ev_name)
    
    if st.session_state.get("target_ev_id") != event.get('event_id'):
        st.session_state.target_ev_id = event.get('event_id')
        st.rerun()

    is_closed = event.get('status') == 'closed'
    is_private_event = event.get('is_private', False)
    
    can_view_details = True
    if is_private_event:
        if user.get("role") not in ["admin", "top_admin"]:
            can_view_details = False

    ev_close_time = event.get('close_time') or event.get('deadline', '')
    if is_closed: 
        st.markdown("<div class='closed-alert' style='background:#ffebee; color:#c62828; padding:10px; border-radius:6px; font-weight:bold; margin-bottom:10px;'>🔒 このイベントは締め切られました。</div>", unsafe_allow_html=True)
    elif ev_close_time: 
        st.markdown(f"<div style='color: #E91E63; font-weight: bold; margin-bottom: 10px;'>⏳ 回答期限: {format_deadline_jp(ev_close_time)}</div>", unsafe_allow_html=True)
        
    if event.get('description'): 
        st.markdown(f"<div class='event-desc'><b>📝 管理者からのメッセージ:</b><br><br>{event['description'].replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)
        
    if is_private_event:
        st.info("🤫 **このイベントはプライベート設定されています。** 管理者以外には、誰が回答したかの名前やコメントは表示されず、全体の人数のみが表示されます。")

    st.markdown("##### 🔗 このイベントの招待URL")
    st.code(f"{APP_BASE_URL}?event={event.get('event_id')}", language="text")

    event_type = event.get('type') or event.get('event_type', 'time')

    # ＝＝＝＝＝ 🕒 時間帯 / 🏫 時間割 モード ＝＝＝＝＝
    if event_type in ['time', 'timetable']:
        if event_type == 'time':
            s_idx = int(event.get('start_time_idx') or event.get('start_idx', 0))
            e_idx = int(event.get('end_time_idx') or event.get('end_idx', 0))
            
            # 古いイベントなどでバグがある場合の安全装置
            if e_idx <= s_idx: e_idx = s_idx + 1
            
            date_objs = []
            try:
                curr = pd.to_datetime(event.get('start_date', ''), errors='coerce').date()
                end_d = pd.to_datetime(event.get('end_date', ''), errors='coerce').date()
                if pd.isna(curr) or pd.isna(end_d): raise Exception
            except:
                curr = datetime.today().date()
                end_d = curr + timedelta(days=7)
                
            while curr <= end_d: date_objs.append(curr); curr += timedelta(days=1)
            date_strs = [d.strftime("%Y-%m-%d") for d in date_objs]
            clean_date_labels = [f"{d.strftime('%m/%d')}({['月','火','水','木','金','土','日'][d.weekday()]})" for d in date_objs]
            
            # 💡 「〜18:00」なら「17:45のマス」が最後になるように描画
            time_labels = [idx_to_time(i) for i in range(s_idx, e_idx)]
            
            week_nav_display = "flex"

            fixed_sched = user.get("fixed_schedule", {})
            try: fixed_locs = json.loads(user.get("group_4", "{}"))
            except: fixed_locs = {}

            unavail_col_rows = {}
            for c, date_obj in enumerate(date_objs):
                wd = str(date_obj.weekday())
                if wd in fixed_sched:
                    day_bin = fixed_sched[wd]
                    unavail_global_idxs = [i for i, bit in enumerate(day_bin) if bit == '1']
                    unavail_rows = []
                    for gi in unavail_global_idxs:
                        if s_idx <= gi < e_idx:
                            campus = ""
                            if 36 <= gi < 42: campus = fixed_locs.get(wd, {}).get("p1", "")
                            elif 43 <= gi < 49: campus = fixed_locs.get(wd, {}).get("p2", "")
                            elif 53 <= gi < 59: campus = fixed_locs.get(wd, {}).get("p3", "")
                            elif 60 <= gi < 66: campus = fixed_locs.get(wd, {}).get("p4", "")
                            elif 67 <= gi < 73: campus = fixed_locs.get(wd, {}).get("p5", "")
                            elif 74 <= gi: campus = fixed_locs.get(wd, {}).get("af", "")
                            unavail_rows.append({"row": gi - s_idx, "campus": campus})
                    if unavail_rows: unavail_col_rows[str(c)] = unavail_rows
                    
        else: # 🏫 timetable モード
            s_idx, e_idx = 0, 6 # 5ではなく6要素ぶん
            date_strs = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            clean_date_labels = ["月曜", "火曜", "水曜", "木曜", "金曜"]
            time_labels = ["1限", "2限", "3限", "4限", "5限", "放課後"]
            cell_h = "50px"
            week_nav_display = "none"
            
            fixed_sched = user.get("fixed_schedule", {})
            try: fixed_locs = json.loads(user.get("group_4", "{}"))
            except: fixed_locs = {}

            unavail_col_rows = {}
            for c, wd in enumerate(["0", "1", "2", "3", "4"]):
                if wd in fixed_sched:
                    day_bin = fixed_sched[wd]
                    u_rows = []
                    if "1" in day_bin[36:42]: u_rows.append({"row": 0, "campus": fixed_locs.get(wd, {}).get("p1", "")})
                    if "1" in day_bin[43:49]: u_rows.append({"row": 1, "campus": fixed_locs.get(wd, {}).get("p2", "")})
                    if "1" in day_bin[53:59]: u_rows.append({"row": 2, "campus": fixed_locs.get(wd, {}).get("p3", "")})
                    if "1" in day_bin[60:66]: u_rows.append({"row": 3, "campus": fixed_locs.get(wd, {}).get("p4", "")})
                    if "1" in day_bin[67:73]: u_rows.append({"row": 4, "campus": fixed_locs.get(wd, {}).get("p5", "")})
                    if "1" in day_bin[74:]: u_rows.append({"row": 5, "campus": fixed_locs.get(wd, {}).get("af", "")})
                    if u_rows: unavail_col_rows[str(c)] = u_rows

        if "df_input" not in st.session_state or st.session_state.get("last_build_ev_id") != event.get('event_id'):
            df = pd.DataFrame(0, index=time_labels, columns=date_strs)
            my_comment = ""
            for r in st.session_state.event_responses:
                if str(r.get('user_id')) == str(user.get('user_id')):
                    d_id = r.get('date'); my_comment = r.get('comment', "")
                    if d_id in date_strs:
                        b_str = str(r.get('binary_data') or r.get('binary', "")).replace("'", "").zfill(96)
                        for i in range(len(time_labels)):
                            if event_type == 'time': v = int(b_str[s_idx + i]) if (s_idx + i) < len(b_str) else 0
                            else: v = int(b_str[i]) if i < len(b_str) else 0
                            df.loc[time_labels[i], d_id] = v
                            
            st.session_state.df_input = df
            st.session_state.my_comment = my_comment
            st.session_state.last_build_ev_id = event.get('event_id')

        if event_type == 'time':
            st.markdown("<div style='margin-bottom: 5px; font-weight: bold; color: #333;'>🔍 カレンダーの表示サイズ（マスの縦幅）</div>", unsafe_allow_html=True)
            col_z1, col_z2 = st.columns([1.5, 1])
            with col_z1: zoom_mode = st.radio("表示サイズ", ["小 (全体を俯瞰)", "標準", "大 (タップしやすい)"], index=1, horizontal=True, label_visibility="collapsed")
            with col_z2: st.markdown("<div style='font-size:11px; color:#888; margin-top:8px;'>※変更すると未保存の入力はリセットされます。</div>", unsafe_allow_html=True)
            if zoom_mode.startswith("小"): cell_h = "20px"
            elif zoom_mode.startswith("大"): cell_h = "50px"
            else: cell_h = "36px"

        tab_in, tab_graph = st.tabs(["📅 入力", "📊 集計"])
        with tab_in:
            if event_type == 'time':
                st.markdown("##### 📅 カレンダー連携")
                c_import1, c_import2 = st.columns([3, 1])
                with c_import1:
                    st.write("プロフィールに設定したカレンダーの予定を読み込んで、自動で「授業等(グレー)」として反映します。")
                with c_import2:
                    if st.button("🔄 カレンダーから取得", use_container_width=True):
                        user_cal_url = user.get('calendar_url', '')
                        if not user_cal_url or "http" not in user_cal_url:
                            st.warning("プロフィール画面でカレンダーの非公開URLを設定してください。")
                        else:
                            with st.spinner("予定を取得中..."):
                                res = call_gas("import_google_calendar", {
                                    "payload": {
                                        "start_date": event.get('start_date'),
                                        "end_date": event.get('end_date'),
                                        "ical_url": user_cal_url
                                    }
                                }, method="POST")
                                
                                if res.get("status") == "success":
                                    busy_data = res.get("data", {}).get("busy_slots", {})
                                    if not busy_data:
                                        st.info("指定期間に予定は見つかりませんでした。")
                                    else:
                                        for d_str, slots in busy_data.items():
                                            if d_str in st.session_state.df_input.columns:
                                                for t_idx in slots:
                                                    if s_idx <= t_idx < e_idx:
                                                        time_label = time_master[t_idx]
                                                        if time_label in st.session_state.df_input.index:
                                                            st.session_state.df_input.loc[time_label, d_str] = 3
                                        st.success("カレンダーの予定を反映しました！内容を確認して「提出」を押してください。")
                                        time.sleep(1)
                                        st.rerun()
                                else:
                                    st.error("取得に失敗しました。GAS側の権限承認が済んでいるか確認してください。")

            st.markdown("---")

            user_campuses = [x.strip() for x in str(user.get('group_1', '')).split(',') if x.strip()]
            default_campus_initial = user_campuses[0] if user_campuses else "なかもず"
            campus_options = MASTER_G1 + ["その他/移動中"]
            default_index = campus_options.index(default_campus_initial) if default_campus_initial in campus_options else 0
            
            st.markdown("##### 📍 今回のデフォルト所在地")
            selected_default_campus = st.selectbox("「可」を塗った時に自動で設定されるキャンパス", campus_options, index=default_index)

            st.markdown(campus_legend_html, unsafe_allow_html=True)

            m = st.session_state.df_input[date_strs].values.tolist()
            time_opts_html = "".join([f'<option value="{i}">{t}</option>' for i, t in enumerate(time_labels)])
            
            # --- 💡 終了時刻用に +15分 したラベルを生成 ---
            if event_type == 'time':
                end_time_labels = [idx_to_time(time_master.index(t) + 1) if time_master.index(t) < 95 else "24:00" for t in time_labels]
                time_opts_end_html = "".join([f'<option value="{i}">{t}</option>' for i, t in enumerate(end_time_labels)])
            else:
                time_opts_end_html = time_opts_html
            
            src_opts_html = "".join([f'<option value="{i}">{l}</option>' for i, l in enumerate(clean_date_labels)])
            b_day_opts_html = "".join([f'<label class="ms-opt"><input type="checkbox" class="b-day-chk" value="{i}" checked> {l}</label>' for i, l in enumerate(clean_date_labels)])
            c_tgt_opts_html = "".join([f'<label class="ms-opt"><input type="checkbox" class="c-tgt-chk" value="{i}"> {l}</label>' for i, l in enumerate(clean_date_labels)])
            
            day_cols_html = ""
            for c, d_str in enumerate(date_strs):
                lbl = clean_date_labels[c].replace("(", "<br>(")
                cells_html = ""
                for r, t_str in enumerate(time_labels):
                    val = int(m[r][c])
                    if val == 1: bg, bg_img = "#4CAF50", "none"
                    elif val == 2: bg, bg_img = "#FFEB3B", "none"
                    elif val == 3: bg, bg_img = "#e0e0e0", "repeating-linear-gradient(45deg, transparent, transparent 5px, rgba(255,255,255,.7) 5px, rgba(255,255,255,.7) 10px)"
                    else: bg, bg_img = "#fff", "none"
                    
                    b_top = get_border_top(t_str, event_type)
                    cells_html += f'<div class="c" data-r="{r}" data-c="{c}" data-v="{val}" style="height:{cell_h}; background:{bg}; background-image:{bg_img}; cursor:pointer; border-top:{b_top}; border-right:1px solid #eee; box-sizing:border-box;"></div>'
                day_cols_html += f'<div class="day-col" data-c="{c}" style="flex:1; min-width:85px; box-sizing:border-box; display:none;"><div class="header-cell">{lbl}</div>{cells_html}</div>'

            time_cells_html = ""
            for r, t_str in enumerate(time_labels):
                b_top = get_border_top(t_str, event_type)
                lbl = t_str if t_str.endswith(":00") or t_str.endswith(":30") or event_type == 'timetable' else ""
                time_cells_html += f'<div style="background:#f0f2f6; text-align:center; font-size:12px; font-weight:bold; color:#555; height:{cell_h}; line-height:{cell_h}; border-top:{b_top}; border-right:1px solid #ccc; box-sizing:border-box;">{lbl}</div>'
            time_col_html = f'<div class="time-col"><div class="top-left-cell"></div>{time_cells_html}</div>'

            tools_html, submit_btn_html, pointer_css = "", "", ""
            if not is_closed:
                tt_btn_text = "🚫 該当日の自分の時間割をすべて × にする" if event_type == "time" else "🚫 自分の時間割をそのまま反映する"
                tools_html = f"""
                <div style="display:flex; gap:15px; flex-wrap:wrap; margin-bottom: 20px;">
                    <div class="tool-card" style="display:{'none' if event_type == 'timetable' else 'block'};"><div class="tool-header">🪄 一括指定ツール</div>
                        <div style="display:flex; gap:10px; margin-bottom:10px; align-items:center; flex-wrap:wrap;">状態: <select id="b-val" class="st-sel"><option value="1">可 (緑)</option><option value="2">未定 (黄)</option><option value="0">不可 (白)</option></select>時間: <select id="b-start" class="st-sel">{time_opts_html}</select> 〜 <select id="b-end" class="st-sel"><option value="{len(time_labels)-1}" selected>{end_time_labels[-1] if event_type == "time" else time_labels[-1]}</option>{time_opts_end_html}</select></div>
                        <div style="display:flex; gap:10px; align-items:center;">対象: <div class="ms-container"><div class="ms-header" onclick="window.toggleList('b-days-list');">対象日を選択 <span>▼</span></div><div id="b-days-list" class="ms-options" style="display:none;"><label class="ms-opt" style="font-weight:bold;"><input type="checkbox" onchange="document.querySelectorAll('.b-day-chk').forEach(c => c.checked = this.checked)" checked> 全て選択 / 解除</label><hr style="margin:5px 0; border:0; border-top:1px solid #ccc;">{b_day_opts_html}</div></div><button class="st-btn" onclick="window.doBulk(this)">適用</button></div>
                    </div>
                    <div class="tool-card"><div class="tool-header">📋 日程コピー機能</div>
                        <div style="display:flex; gap:10px; margin-bottom:10px; align-items:center;">元: <select id="c-src" class="st-sel" style="flex:1;">{src_opts_html}</select></div>
                        <div style="display:flex; gap:10px; align-items:center;">先: <div class="ms-container"><div class="ms-header" onclick="window.toggleList('c-tgt-list');">対象日を選択 <span>▼</span></div><div id="c-tgt-list" class="ms-options" style="display:none;"><label class="ms-opt" style="font-weight:bold;"><input type="checkbox" onchange="document.querySelectorAll('.c-tgt-chk').forEach(c => c.checked = this.checked)"> 全て選択 / 解除</label><hr style="margin:5px 0; border:0; border-top:1px solid #ccc;">{c_tgt_opts_html}</div></div><button class="st-btn" onclick="window.doCopy(this)" style="background:#FF9800;">コピー実行</button></div>
                    </div>
                    <div class="tool-card"><div class="tool-header">⏰ 時間割パワー反映</div>
                        <p style="font-size:12px; color:#555; margin:0 0 10px 0;">DBに保存された時間割を展開し、場所情報付きで自動反映させます。</p>
                        <button class="st-btn" onclick="window.doTimetable(this)" style="background:#E91E63; width:100%;">{tt_btn_text}</button>
                    </div>
                </div>"""
                submit_btn_html = f"""
                <div style="margin-top: 20px;">
                    <label style="font-size: 14px; font-weight: 600; color: #333;">📝 全体へのコメント (遅刻・早退など)</label>
                    <textarea id="comment-box" rows="2" style="width: 100%; padding: 10px; margin-top: 5px; border: 1px solid #ccc; border-radius: 6px; font-family: sans-serif; resize: vertical;">{st.session_state.my_comment}</textarea>
                </div>
                <button id="submit-btn" style="margin-top: 15px; width: 100%; padding: 14px; background-color: #FF4B4B; color: white; border: none; border-radius: 8px; font-size: 16px; cursor: pointer; font-weight: 600; box-shadow: 0 4px 6px rgba(0,0,0,0.15); transition: 0.2s;">✅ すべて記入して提出 (全体が保存されます)</button>
                """
            else:
                pointer_css = "pointer-events: none; opacity: 0.8;"
                submit_btn_html = f"""<div style="margin-top: 20px;"><label style="font-size: 14px; font-weight: 600; color: #333;">📝 全体へのコメント</label><div style="width: 100%; padding: 10px; margin-top: 5px; background: #eee; border: 1px solid #ccc; border-radius: 6px; font-family: sans-serif; min-height:40px;">{st.session_state.my_comment}</div></div>"""

            # 💡 カレンダーの空白余白をなくす（高さをAutoに）
            scroll_css = "max-height: 650px; height: auto;"

            html_code = f"""
            <style>
                .tool-card {{ background: #fdfdfd; padding: 15px; border: 1px solid #e0e0e0; border-radius: 8px; flex: 1; min-width: 250px; font-family: sans-serif; box-sizing:border-box;}} 
                .tool-header {{ font-size: 15px; font-weight: bold; color: #333; margin-bottom: 12px; }} 
                .st-sel {{ padding: 6px; border: 1px solid #ccc; border-radius: 4px; font-family: inherit; font-size: 13px; }} 
                .st-btn {{ padding: 6px 16px; border: none; border-radius: 4px; background: #4CAF50; color: white; cursor: pointer; font-weight: bold; transition: 0.2s; font-size: 13px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);}} 
                .st-btn:hover {{ opacity: 0.9; }} 
                .ms-container {{ position: relative; display: inline-block; flex: 1; min-width: 150px; }} 
                .ms-header {{ border: 1px solid #ccc; padding: 6px 10px; border-radius: 4px; cursor: pointer; background: #fff; font-size: 13px; user-select: none; display: flex; justify-content: space-between; }} 
                .ms-options {{ position: absolute; top: 100%; left: 0; right: 0; background: #fff; border: 1px solid #ccc; border-radius: 4px; z-index: 100; max-height: 200px; overflow-y: auto; box-shadow: 0 4px 6px rgba(0,0,0,0.1); padding: 5px; margin-top: 2px; }} 
                .ms-opt {{ display: block; padding: 6px 8px; font-size: 13px; cursor: pointer; border-radius: 3px; }} 
                .ms-opt:hover {{ background: #f0f2f6; }} 
                .page-btn {{ padding: 8px 16px; border: 1px solid #ccc; background: #fff; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 14px; transition: 0.2s; }} 
                .page-btn:hover:not(:disabled) {{ background: #e9ecef; }} 
                .page-btn:disabled {{ opacity: 0.4; cursor: not-allowed; }}
                
                .scroll-wrapper {{ {scroll_css} overflow: auto; border: 1px solid #ccc; border-radius: 6px; position: relative; background: #fff; }}
                .time-col {{ position: sticky; left: 0; z-index: 10; background: #f0f2f6; box-shadow: 2px 0 5px rgba(0,0,0,0.1); flex-shrink: 0; width: 65px; box-sizing: border-box; }}
                .header-cell {{ position: sticky; top: 0; z-index: 11; background: #eee; text-align: center; font-size: 13px; padding: 5px 0; font-weight: bold; border-bottom: 2px solid #555; border-right: 1px solid #ccc; height: 50px; box-sizing: border-box; display: flex; align-items: center; justify-content: center; line-height: 1.2; }}
                .top-left-cell {{ position: sticky; top: 0; left: 0; z-index: 20; background: #f0f2f6; border-right: 1px solid #ccc; border-bottom: 2px solid #555; height: 50px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); box-sizing: border-box; }}
            </style>
            {tools_html}
            <div style='display:flex; justify-content:flex-end; align-items:flex-end; margin-bottom:10px;'>
                <div style="display:{week_nav_display}; align-items:center; gap: 20px;">
                    <button id="btn-prev" class="page-btn" onclick="window.changeWeek(-1)">◀ 前の週</button>
                    <div style="font-weight:bold; color:#4CAF50;">📅 全 {len(date_strs)} 日間</div>
                    <button id="btn-next" class="page-btn" onclick="window.changeWeek(1)">次の週 ▶</button>
                </div>
            </div>
            <div class="scroll-wrapper">
                <div id="g" style="display:flex; width:100%; user-select:none; {pointer_css}">
                    {time_col_html}
                    {day_cols_html}
                </div>
            </div>
            {submit_btn_html}
            """
            
            my_cell_details = {}
            for r in st.session_state.event_responses:
                if str(r.get('user_id')) == str(user.get('user_id')) and r.get('cell_details'):
                    try:
                        my_cell_details = json.loads(r['cell_details'])
                        break
                    except:
                        pass

            raw = grid_editor(
                html_code=html_code, 
                rows=len(time_labels), 
                cols=len(date_strs), 
                eventId=event.get('event_id'), 
                isClosed=is_closed, 
                unavailColRows=unavail_col_rows, 
                saveTs=st.session_state.get("last_saved_ts", 0), 
                cellDetails=my_cell_details, 
                defaultCampus=selected_default_campus, 
                default=None, 
                key=f"editor_{event.get('event_id')}"
            )
            
            if raw and isinstance(raw, dict) and "data" in raw:
                if raw.get("trigger_save") and st.session_state.get("last_saved_ts") != raw.get("ts"):
                    st.session_state.last_saved_ts = raw.get("ts")
                    st.session_state.df_input = pd.DataFrame(raw["data"], index=time_labels, columns=date_strs)
                    st.session_state.my_comment = raw.get("comment", "")
                    
                    cell_details_json = raw.get("cell_details", {})
                    cell_details_str = json.dumps(cell_details_json, separators=(',', ':')) if cell_details_json else "{}"
                    
                    all_res = []
                    for d_id in date_strs:
                        bits = ["0"] * 96
                        has_data = False
                        for t_idx in range(len(time_labels)): 
                            val = int(st.session_state.df_input.loc[time_labels[t_idx], d_id])
                            if val > 0: has_data = True
                            if event_type == 'time': bits[s_idx + t_idx] = str(val)
                            else: bits[t_idx] = str(val)
                            
                        if has_data:
                            all_res.append({"date": d_id, "binary_data": "".join(bits)})
                            
                    if not all_res:
                        all_res.append({"date": date_strs[0], "binary_data": "0"*96})

                    payload = {
                        "event_id": event.get("event_id"), 
                        "user_id": user.get("user_id"), 
                        "comment": st.session_state.my_comment, 
                        "cell_details": cell_details_str, 
                        "responses": all_res
                    }
                    if save_response_hybrid(payload):
                        st.session_state.save_success_msg = "回答を保存しました！"
                        st.rerun()

        with tab_graph:
            st.subheader("📊 全体の集計結果")
            col1, col2 = st.columns([2, 1])
            with col1: policy = st.radio("「未定(△)」の計算方法", [0.5, 1.0, 0.0], format_func=lambda x: f"{x}人としてカウント", horizontal=True)
            with col2:
                if st.button("🔄 最新の回答を取得", use_container_width=True):
                    st.rerun()

            all_res_data = st.session_state.event_responses
            all_names = list(set([r.get('user_name', '不明') for r in all_res_data]))
            all_g1, all_g2, all_g3 = set(), set(), set()
            for r in all_res_data:
                for g in str(r.get('group_1', '')).split(','):
                    if g.strip(): all_g1.add(g.strip())
                for g in str(r.get('group_2', '')).split(','):
                    if g.strip(): all_g2.add(g.strip())
                for g in str(r.get('group_3', '')).split(','):
                    if g.strip(): all_g3.add(g.strip())

            all_g1_sorted = sort_groups(list(all_g1), MASTER_G1)
            all_g2_sorted = sort_groups(list(all_g2), MASTER_G2)
            all_g3_sorted = sort_groups(list(all_g3), MASTER_G3)

            with st.expander("🔍 絞り込みフィルター（回答者・所在地・時間帯・日付）", expanded=False):
                with st.form("filter_form"):
                    st.markdown("<span style='font-size:14px; color:#555;'>指定した条件に合致するデータだけをグラフに表示します。（未選択の場合はすべて表示）</span>", unsafe_allow_html=True)
                    
                    st.markdown("##### 👥 回答者・所在地")
                    f_col1, f_col2 = st.columns(2)
                    with f_col1:
                        f_g1 = st.multiselect("🏫 現在通っているキャンパス (プロフィール)", all_g1_sorted)
                        f_locs = st.multiselect("📍 所在地 (回答時に指定したキャンパス)", MASTER_G1 + ["その他/移動中"])
                    with f_col2:
                        f_g2 = st.multiselect("🎓 入学年度", all_g2_sorted)
                        f_g3 = st.multiselect("🤝 オプション", all_g3_sorted)
                        f_names = st.multiselect("👤 特定の個人", sorted(all_names))
                        
                    st.markdown("---")
                    st.markdown("##### ⏰ 時間帯")
                    f_time = st.select_slider("グラフに表示する時間帯を切り取る", options=time_labels, value=(time_labels[0], time_labels[-1]))
                    
                    if event_type == 'time':
                        st.markdown("##### 📅 日付・曜日")
                        f_d_col1, f_d_col2 = st.columns(2)
                        with f_d_col1:
                            f_dates = st.date_input("表示する期間", value=(date_objs[0], date_objs[-1]), min_value=date_objs[0], max_value=date_objs[-1])
                        with f_d_col2:
                            f_wdays = st.multiselect("表示する曜日", ["月", "火", "水", "木", "金", "土", "日"], default=["月", "火", "水", "木", "金", "土", "日"])
                    else:
                        f_dates = None
                        f_wdays = ["月", "火", "水", "木", "金"]

                    submitted = st.form_submit_button("✅ フィルターを適用して集計", type="primary")

            filtered_data = []
            for r in all_res_data:
                u_g1 = [x.strip() for x in str(r.get('group_1', '')).split(',') if x.strip()]
                u_g2 = [x.strip() for x in str(r.get('group_2', '')).split(',') if x.strip()]
                u_g3 = [x.strip() for x in str(r.get('group_3', '')).split(',') if x.strip()]
                if f_names and r.get('user_name') not in f_names: continue
                if f_g1 and not set(f_g1).intersection(set(u_g1)): continue
                if f_g2 and not set(f_g2).intersection(set(u_g2)): continue
                if f_g3 and not set(f_g3).intersection(set(u_g3)): continue
                
                if f_locs:
                    user_locs = set()
                    if r.get('cell_details') and str(r.get('cell_details')).strip() not in ["", "{}"]:
                        try:
                            cd = json.loads(r['cell_details'])
                            for k, v in cd.items():
                                if v.get('campus'):
                                    user_locs.add(v['campus'])
                        except:
                            pass
                    if not set(f_locs).intersection(user_locs):
                        continue
                
                filtered_data.append(r)
            
            t_start_idx = time_labels.index(f_time[0])
            t_end_idx = time_labels.index(f_time[1])
            disp_time_labels = time_labels[t_start_idx : t_end_idx + 1]

            disp_date_strs = []
            disp_clean_date_labels = []
            if event_type == 'time':
                d_start = f_dates[0] if isinstance(f_dates, tuple) and len(f_dates) > 0 else date_objs[0]
                d_end = f_dates[1] if isinstance(f_dates, tuple) and len(f_dates) > 1 else d_start
                for c, d_obj in enumerate(date_objs):
                    wd_str = ["月", "火", "水", "木", "金", "土", "日"][d_obj.weekday()]
                    if d_start <= d_obj <= d_end and wd_str in f_wdays:
                        disp_date_strs.append(date_strs[c])
                        disp_clean_date_labels.append(clean_date_labels[c])
            else:
                disp_date_strs = date_strs
                disp_clean_date_labels = clean_date_labels

            unique_all = len(set([r.get('user_id') for r in all_res_data]))
            unique_filtered = len(set([r.get('user_id') for r in filtered_data]))
            if unique_all != unique_filtered:
                st.info(f"🔍 フィルター適用中： 回答者 **{unique_all}人** 中、条件に合う **{unique_filtered}人** のデータを集計しています。")

            if st.checkbox("集計グラフを表示", value=True):
                st.markdown(campus_legend_html, unsafe_allow_html=True)
                
                if not disp_date_strs or not disp_time_labels:
                    st.warning("⚠️ 指定された条件に合う日付または時間帯がありません。フィルター条件を広げてください。")
                else:
                    z = np.zeros((len(disp_time_labels), len(disp_date_strs)))
                    h = [["" for _ in range(len(disp_date_strs))] for _ in range(len(disp_time_labels))]
                    comments_list = []
                    
                    for r in filtered_data:
                        if r.get('date') not in disp_date_strs:
                            continue
                            
                        c_idx = disp_date_strs.index(r['date'])
                        
                        b = str(r.get('binary_data') or r.get('binary', "")).replace("'", "").zfill(96)
                        
                        cd = {}
                        if r.get('cell_details') and str(r.get('cell_details')).strip() not in ["", "{}"]:
                            try: cd = json.loads(r['cell_details'])
                            except: pass
                            
                        u_campuses = [x.strip() for x in str(r.get('group_1', '')).split(',') if x.strip()]
                        u_default_campus = u_campuses[0] if u_campuses else ""

                        if can_view_details and r.get('comment') and r.get('comment').strip() != "":
                            if {"user": r.get('user_name'), "comment": r.get('comment')} not in comments_list: 
                                comments_list.append({"user": r.get('user_name'), "comment": r.get('comment')})
                        
                        for disp_r, t_str in enumerate(disp_time_labels):
                            orig_r_idx = time_labels.index(t_str)
                            orig_v = int(b[s_idx + orig_r_idx]) if event_type == 'time' else int(b[orig_r_idx])
                            
                            v = 0 if orig_v == 3 else orig_v
                            
                            cell_campus = u_default_campus if orig_v in [1, 2] else ""
                            cell_note = ""
                            
                            cell_key = f"{orig_r_idx}_{c_idx}"
                            if cell_key in cd:
                                if cd[cell_key].get('campus'): cell_campus = cd[cell_key]['campus']
                                if cd[cell_key].get('note'): cell_note = cd[cell_key]['note']
                            
                            if f_locs and orig_v in [1, 2, 3]:
                                if cell_campus not in f_locs:
                                    continue 
                                    
                            z[disp_r, c_idx] += (1.0 if v==1 else policy if v==2 else 0.0)
                            
                            if can_view_details and orig_v in [1, 2, 3]:
                                campus_str = f" ({cell_campus})" if cell_campus else ""
                                note_str = f" <span style='color:#FFEB3B; font-size:10.5px;'>[{cell_note}]</span>" if cell_note else ""
                                name_html = f"{r.get('user_name', '')}<span style='font-size:10.5px; color:#bbb;'>{campus_str}</span>{note_str}"
                                
                                if orig_v==1: h[disp_r][c_idx] += f"◯ {name_html}<br>"
                                elif orig_v==2: h[disp_r][c_idx] += f"△ {name_html}<br>"
                                elif orig_v==3: h[disp_r][c_idx] += f"<span style='color:#aaa;'>📓 {name_html}</span><br>"
                    
                    max_z = np.max(z) if np.max(z) > 0 else 1
                    
                    agg_time_cells = ""
                    for r, t_str in enumerate(disp_time_labels):
                        b_top = get_border_top(t_str, event_type)
                        lbl = t_str if t_str.endswith(":00") or t_str.endswith(":30") or event_type == 'timetable' else ""
                        agg_time_cells += f'<div class="agg-time-cell" style="border-top:{b_top}; height:{cell_h};">{lbl}</div>'
                        
                    agg_time_col = f'<div class="agg-time-col"><div class="agg-top-left"></div>{agg_time_cells}</div>'
                    
                    agg_day_cols = ""
                    for c, d_str in enumerate(disp_date_strs):
                        lbl = disp_clean_date_labels[c].replace("(", "<br>(")
                        cells_html = ""
                        for r, t_str in enumerate(disp_time_labels):
                            val = z[r][c]
                            b_top = get_border_top(t_str, event_type)
                            if val == 0: bg, txt_color, val_txt = "#ffffff", "#ccc", "-"
                            else:
                                ratio = val / max_z
                                r_col = int(240 - (240 - 46) * ratio); g_col = int(248 - (248 - 125) * ratio); b_col = int(242 - (242 - 50) * ratio)
                                bg, txt_color, val_txt = f"rgb({r_col}, {g_col}, {b_col})", ("#000" if ratio < 0.6 else "#fff"), f"{val:g}"
                            
                            if not can_view_details: tooltip_txt = f"この時間帯は {val_txt} 人が参加可能です"
                            else: tooltip_txt = h[r][c] if h[r][c] else "参加可能者なし"
                                
                            agg_font_size = "11px" if cell_h == "20px" else "15px"
                            tt_class = "tooltip-down" if r < 3 else "tooltip-up"
                            
                            cells_html += f'<div class="agg-cell" style="background:{bg}; color:{txt_color}; border-top:{b_top}; height:{cell_h}; font-size:{agg_font_size};">{val_txt}<span class="{tt_class}">{t_str}<br><b>{val_txt}人</b><br><hr style="margin:4px 0; border:0; border-top:1px solid rgba(255,255,255,0.3);">{tooltip_txt}</span></div>'
                        agg_day_cols += f'<div class="agg-day-col"><div class="agg-header">{lbl}</div>{cells_html}</div>'

                    # 💡 集計グラフの空白余白もなくす（高さをAutoに固定）
                    agg_css = f"""
                    <style>
                    .agg-wrapper {{ max-height: 680px; height: auto; overflow: auto; border: 1px solid #ccc; border-radius: 6px; position: relative; display: flex; background: #fff; padding-bottom: 50px; }}
                    .agg-time-col {{ position: sticky; left: 0; z-index: 10; background: #f0f2f6; box-shadow: 2px 0 5px rgba(0,0,0,0.1); flex-shrink: 0; width: 65px; }}
                    .agg-header {{ position: sticky; top: 0; z-index: 11; background: #eee; font-size: 13px; font-weight: bold; text-align: center; border-bottom: 2px solid #555; border-right: 1px solid #ccc; height: 50px; display: flex; align-items: center; justify-content: center; padding: 0 5px; box-sizing: border-box; line-height: 1.2; }}
                    .agg-top-left {{ position: sticky; top: 0; left: 0; z-index: 20; background: #f0f2f6; border-right: 1px solid #ccc; border-bottom: 2px solid #555; height: 50px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); box-sizing: border-box; }}
                    .agg-day-col {{ flex: 1; min-width: 85px; box-sizing: border-box; }}
                    .agg-cell {{ border-right: 1px solid #eee; display: flex; align-items: center; justify-content: center; font-weight: bold; position: relative; box-sizing: border-box; cursor: pointer; }}
                    
                    .agg-cell .tooltip-up, .agg-cell .tooltip-down {{ visibility: hidden; width: 180px; background-color: rgba(30,30,30,0.95); color: #fff; text-align: left; border-radius: 6px; padding: 10px; position: absolute; z-index: 99999; left: 50%; transform: translateX(-50%); opacity: 0; transition: opacity 0.2s; font-size: 11.5px; font-weight: normal; line-height: 1.5; pointer-events: none; white-space: pre-wrap; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
                    .agg-cell .tooltip-up {{ bottom: 100%; margin-bottom: 8px; }}
                    .agg-cell .tooltip-down {{ top: 100%; margin-top: 8px; }}
                    .agg-cell .tooltip-up::after {{ content: ""; position: absolute; top: 100%; left: 50%; margin-left: -6px; border-width: 6px; border-style: solid; border-color: rgba(30,30,30,0.95) transparent transparent transparent; }}
                    .agg-cell .tooltip-down::after {{ content: ""; position: absolute; bottom: 100%; left: 50%; margin-left: -6px; border-width: 6px; border-style: solid; border-color: transparent transparent rgba(30,30,30,0.95) transparent; }}
                    .agg-cell:hover .tooltip-up, .agg-cell:hover .tooltip-down {{ visibility: visible; opacity: 1; }}
                    
                    .agg-time-cell {{ background: #f0f2f6; font-size: 12px; font-weight: bold; color: #555; display: flex; align-items: center; justify-content: center; border-right: 1px solid #ccc; box-sizing: border-box; }}
                    </style>
                    """
                    st.markdown(f"{agg_css}<div class='agg-wrapper'>{agg_time_col}{agg_day_cols}</div>", unsafe_allow_html=True)
                    
                    if comments_list and can_view_details:
                        st.markdown("### 💬 参加者からのコメント")
                        for c in comments_list: st.info(f"**{c['user']}**: {c['comment']}")

    # ＝＝＝＝＝ 📅 複数の予定 (候補リスト) モード ＝＝＝＝＝
    elif event_type == 'options':
        opts = json.loads(event.get('options_name') or event.get('event_options', '[]'))
        
        if "event_responses" not in st.session_state:
            st.session_state.event_responses = []

        tab_in, tab_graph = st.tabs(["📅 入力", "📊 集計"])
        with tab_in:
            my_ans_row = next((r for r in st.session_state.event_responses if str(r.get('user_id')) == str(user.get('user_id'))), None)
            
            b_data_val = my_ans_row.get('binary_data') or my_ans_row.get('binary', "") if my_ans_row else ""
            my_ans_bin = b_data_val.replace("'", "").zfill(96) if b_data_val else "0" * 96
            my_comment = my_ans_row.get('comment', '') if my_ans_row else ""
            
            st.markdown("##### 📌 各候補の参加可否を選んでください")
            
            raw = options_editor(options=opts, myAnsBin=my_ans_bin, myComment=my_comment, eventId=event.get('event_id'), isClosed=is_closed, saveTs=st.session_state.get("last_saved_ts", 0), key=f"opt_editor_{event.get('event_id')}")
            
            if raw and isinstance(raw, dict) and raw.get("trigger_save") and st.session_state.get("last_saved_ts") != raw.get("ts"):
                st.session_state.last_saved_ts = raw.get("ts")
                b_str = raw.get("binary", "0"*96).ljust(96, "0")[:96]
                user_comment = raw.get("comment", "")
                
                res_data = [{"date": "options", "binary_data": b_str}]
                payload = {
                    "event_id": event.get("event_id"), 
                    "user_id": user.get("user_id"), 
                    "comment": user_comment, 
                    "responses": res_data
                }
                if save_response_hybrid(payload):
                    st.session_state.save_success_msg = "回答を保存しました！"
                    st.rerun()

        with tab_graph:
            st.subheader("📊 予定の集計結果")
            col1, col2 = st.columns([2, 1])
            with col1: policy = st.radio("「未定(△)」の計算方法", [0.5, 1.0, 0.0], format_func=lambda x: f"{x}人としてカウント", horizontal=True, key="opt_policy")
            with col2:
                if st.button("🔄 最新の回答を取得", use_container_width=True, key="opt_refresh"):
                    st.rerun()

            all_res_data = st.session_state.event_responses
            all_names = list(set([r.get('user_name', '不明') for r in all_res_data]))
            all_g1, all_g2, all_g3 = set(), set(), set()
            for r in all_res_data:
                for g in str(r.get('group_1', '')).split(','):
                    if g.strip(): all_g1.add(g.strip())
                for g in str(r.get('group_2', '')).split(','):
                    if g.strip(): all_g2.add(g.strip())
                for g in str(r.get('group_3', '')).split(','):
                    if g.strip(): all_g3.add(g.strip())

            all_g1_sorted = sort_groups(list(all_g1), MASTER_G1)
            all_g2_sorted = sort_groups(list(all_g2), MASTER_G2)
            all_g3_sorted = sort_groups(list(all_g3), MASTER_G3)

            with st.expander("🔍 絞り込みフィルター（回答者）", expanded=False):
                with st.form("opt_filter_form"):
                    st.markdown("<span style='font-size:14px; color:#555;'>指定した条件に合致する人の回答だけを集計します。（未選択の場合は全員）</span>", unsafe_allow_html=True)
                    f_col1, f_col2 = st.columns(2)
                    with f_col1:
                        f_g1 = st.multiselect("🏫 キャンパス", all_g1_sorted, key="f2_g1")
                        f_g3 = st.multiselect("🤝 オプション", all_g3_sorted, key="f2_g3")
                    with f_col2:
                        f_g2 = st.multiselect("🎓 入学年度", all_g2_sorted, key="f2_g2")
                        f_names = st.multiselect("👤 特定の個人", sorted(all_names), key="f2_names")
                    
                    submitted = st.form_submit_button("✅ フィルターを適用して集計", type="primary")

            filtered_data = []
            for r in all_res_data:
                u_g1 = [x.strip() for x in str(r.get('group_1', '')).split(',') if x.strip()]
                u_g2 = [x.strip() for x in str(r.get('group_2', '')).split(',') if x.strip()]
                u_g3 = [x.strip() for x in str(r.get('group_3', '')).split(',') if x.strip()]
                if f_names and r.get('user_name') not in f_names: continue
                if f_g1 and not set(f_g1).intersection(set(u_g1)): continue
                if f_g2 and not set(f_g2).intersection(set(u_g2)): continue
                if f_g3 and not set(f_g3).intersection(set(u_g3)): continue

                filtered_data.append(r)

            unique_all = len(set([r.get('user_id') for r in all_res_data]))
            unique_filtered = len(set([r.get('user_id') for r in filtered_data]))
            if unique_all != unique_filtered:
                st.info(f"🔍 フィルター適用中： 回答者 **{unique_all}人** 中、条件に合う **{unique_filtered}人** のデータを集計しています。")

            counts = [0.0] * len(opts)
            details = [{"yes": [], "maybe": [], "no": []} for _ in range(len(opts))]
            comments_list = []
            
            for r in filtered_data:
                if can_view_details and r.get('comment') and r.get('comment').strip() != "":
                    if {"user": r.get('user_name'), "comment": r.get('comment')} not in comments_list: 
                        comments_list.append({"user": r.get('user_name'), "comment": r.get('comment')})
                
                b = str(r.get('binary_data') or r.get('binary', "")).replace("'", "").zfill(96)
                for i in range(len(opts)):
                    v = int(b[i]) if i < len(b) else 0
                    if v == 1:
                        counts[i] += 1.0
                        if can_view_details: details[i]["yes"].append(r.get('user_name', ''))
                    elif v == 2:
                        counts[i] += policy
                        if can_view_details: details[i]["maybe"].append(r.get('user_name', ''))
                    else:
                        if can_view_details: details[i]["no"].append(r.get('user_name', ''))
                        
            max_c = max(counts) if counts and max(counts) > 0 else 1
            
            for i, opt in enumerate(opts):
                count = counts[i]
                ratio = count / max_c if max_c > 0 else 0
                bar_width = f"{ratio * 100}%"
                
                st.markdown(f"""
                <div style="background:#fff; border:1px solid #e0e0e0; border-radius:12px; padding:15px; margin-bottom:5px; box-shadow:0 4px 6px rgba(0,0,0,0.05);">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <div style="font-size:16px; font-weight:bold; color:#333;">📅 {opt}</div>
                        <div style="font-size:14px; font-weight:bold; color:#4CAF50;">参加目安: {count:g} 人</div>
                    </div>
                    <div style="width:100%; background:#f0f2f6; border-radius:8px; height:12px; overflow:hidden;">
                        <div style="width:{bar_width}; background:linear-gradient(90deg, #81c784, #4CAF50); height:100%; border-radius:8px; transition:width 0.5s;"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                if can_view_details:
                    with st.expander("👥 参加者内訳を見る"):
                        c_yes, c_maybe, c_no = st.columns(3)
                        with c_yes:
                            st.markdown("<span style='color:#4CAF50; font-weight:bold;'>◯ 参加可能</span>", unsafe_allow_html=True)
                            if details[i]["yes"]:
                                st.code("\n".join(details[i]["yes"]), language="text")
                            else:
                                st.write("なし")
                        with c_maybe:
                            st.markdown("<span style='color:#FF9800; font-weight:bold;'>△ 未定</span>", unsafe_allow_html=True)
                            if details[i]["maybe"]:
                                st.markdown("<br>".join([f"△ {n}" for n in details[i]["maybe"]]), unsafe_allow_html=True)
                            else:
                                st.write("なし")
                        with c_no:
                            st.markdown("<span style='color:#F44336; font-weight:bold;'>× 不可</span>", unsafe_allow_html=True)
                            if details[i]["no"]:
                                st.markdown("<br>".join([f"× {n}" for n in details[i]["no"]]), unsafe_allow_html=True)
                            else:
                                st.write("なし")
                
                st.markdown("<div style='height:15px;'></div>", unsafe_allow_html=True)
            
            if comments_list and can_view_details:
                st.markdown("---")
                st.markdown("### 💬 参加者からのコメント")
                for c in comments_list: st.info(f"**{c['user']}**: {c['comment']}")

if __name__ == "__main__":
    main()
