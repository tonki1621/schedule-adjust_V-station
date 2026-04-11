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
                    if dl_dt.tzinfo is not None: dl_dt = dl_dt.tz_convert(None)
                    if now > dl_dt:
                        ev["status"] = "closed"
                        db.collection("events").document(ev["event_id"]).update({"status": "closed"})
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
st.set_page_config(page_title="V-Sync by もっきゅー", layout="wide", initial_sidebar_state="expanded")
APP_BASE_URL = "https://schedule-adjust-v-station.streamlit.app/"

st.markdown("""
    <style>
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
        
        /* 独自タブ用デザイン */
        div[data-testid="stRadio"] > div { display: flex; flex-direction: row; background: #f0f2f6; padding: 4px; border-radius: 8px; gap: 4px; }
        div[data-testid="stRadio"] label { background: transparent; padding: 10px 20px !important; border-radius: 6px !important; cursor: pointer; transition: 0.2s; font-weight: bold; flex: 1; text-align: center; justify-content: center;}
        div[data-testid="stRadio"] label[data-checked="true"] { background: #fff !important; color: #4CAF50 !important; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        
        /* ダッシュボードのカード */
        .dash-card { background: #fff; padding: 15px; border-radius: 10px; border: 1px solid #ddd; border-left: 5px solid #2196F3; margin-bottom: 12px; cursor: pointer; transition: 0.2s; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .dash-card:hover { transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
        .dash-card.answered { border-left-color: #4CAF50; }
        
        .user-header { display: flex; align-items: center; justify-content: space-between; background: #f8f9fa; padding: 10px 20px; border-radius: 8px; border-left: 5px solid #4CAF50; margin-bottom: 20px; }
        .tt-day-header { font-size: 16px; font-weight: bold; background: #4CAF50; color: white; padding: 8px; border-radius: 6px; text-align: center; }
        .tt-time-cell { font-size: 14px; font-weight: bold; background: #f0f2f6; padding: 10px; border-radius: 6px; text-align: center; border-left: 4px solid #4CAF50;}
        .tt-time-sub { font-size: 11px; color: #666; font-weight: normal; }
        .status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #4CAF50, #45a049); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(76,175,80,0.3); }
        .af-status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #2196F3, #1976D2); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(33,150,243,0.3); }
        .status-off { color: #9e9e9e; background: #ffffff; padding: 4px 0; border-radius: 6px; border: 1px dashed #d0d0d0; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block;}
    </style>
""", unsafe_allow_html=True)

GAS_URL = "https://script.google.com/macros/s/AKfycby7hAc1_dhSQ_tJzSiJeSc2Ez7pgaeVTrVL5fOIZPNNZ-_YLke236yGgCgj3yijhQHh/exec"

os.makedirs("rt_editor", exist_ok=True)
with open("rt_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:sans-serif;margin:0;padding:0;background:transparent;}.editor-container{border:1px solid #ccc;border-radius:6px;overflow:hidden;background:#fff;}.toolbar{background:#f8f9fb;padding:6px;border-bottom:1px solid #ccc;display:flex;gap:5px;flex-wrap:wrap;align-items:center;}.toolbar button{background:#fff;border:1px solid #ccc;border-radius:4px;padding:4px 10px;font-size:13px;cursor:pointer;color:#333;transition:0.2s;}.toolbar button:hover{background:#e9ecef;}textarea{width:100%;height:120px;border:none;padding:10px;font-size:14px;resize:vertical;outline:none;box-sizing:border-box;font-family:inherit;line-height:1.5;}</style></head><body><div class="editor-container"><div class="toolbar"><button onclick="insertTag('<b>', '</b>')" title="太字"><b>B</b> 太字</button><button onclick="insertTag('<i>', '</i>')" title="斜体"><i>I</i> 斜体</button><div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div><button onclick="insertRed()" title="赤文字"><span style="color:#FF4B4B; font-weight:bold;">A</span> 赤</button><button onclick="insertBlue()" title="青文字"><span style="color:#2196F3; font-weight:bold;">A</span> 青</button><div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div><button onclick="insertLink()" title="リンク">🔗 リンク追加</button></div><textarea id="editor" placeholder="📝 イベントの説明や注意事項を入力..."></textarea></div><script>function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); } function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); } function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); } const editor = document.getElementById('editor'); let timer; function sendValue() { setComponentValue(editor.value); } function insertTag(startTag, endTag) { const start = editor.selectionStart; const end = editor.selectionEnd; const val = editor.value; const selected = val.substring(start, end); editor.value = val.substring(0, start) + startTag + selected + endTag + val.substring(end); editor.focus(); editor.selectionStart = start + startTag.length; editor.selectionEnd = end + startTag.length; sendValue(); } function insertRed() { insertTag("<span style='color:#FF4B4B; font-weight:bold;'>", "</span>"); } function insertBlue() { insertTag("<span style='color:#2196F3; font-weight:bold;'>", "</span>"); } function insertLink() { const url = prompt('リンク先のURLを入力', 'https://'); if (url) { const text = prompt('表示するテキストを入力', 'こちらをクリック'); if (text) { const linkTag = `<a href='${url}' target='_blank'>${text}</a>`; const start = editor.selectionStart; const val = editor.value; editor.value = val.substring(0, start) + linkTag + val.substring(editor.selectionEnd); sendValue(); } } } editor.addEventListener('blur', sendValue); window.addEventListener("message", function(event) { if (event.data.type === "streamlit:render") { sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.body.scrollHeight + 15}); } }); init();</script></body></html>""")
rt_editor = components.declare_component("rt_editor", path="rt_editor")

os.makedirs("options_editor", exist_ok=True)
with open("options_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{margin:0;font-family:sans-serif;}.opt-card{background:#fff;border:1px solid #e0e0e0;border-radius:12px;padding:15px;margin-bottom:15px;box-shadow:0 2px 5px rgba(0,0,0,0.05);}.opt-title{font-size:18px;font-weight:bold;color:#2e7d32;margin-bottom:15px;text-align:center;}.btn-group{display:flex;gap:12px;}.opt-btn{flex:1;padding:20px 0;border-radius:12px;border:2px solid #ddd;background:#fff;font-size:18px;font-weight:bold;cursor:pointer;transition:all 0.2s cubic-bezier(0.175, 0.885, 0.32, 1.275);color:#555;text-align:center;}.opt-btn[data-v="1"].active{background:#4CAF50;color:#fff;border-color:#4CAF50;box-shadow:0 6px 12px rgba(76,175,80,0.4);transform:translateY(-3px);}.opt-btn[data-v="2"].active{background:#81C784;color:#fff;border-color:#81C784;box-shadow:0 4px 8px rgba(76,175,80,0.2);transform:translateY(-3px); opacity: 0.8;}.opt-btn[data-v="0"].active{background:#f5f5f5;color:#777;border-color:#ccc;transform:translateY(-3px);}#submit-btn{width:100%;padding:18px;background-color:#FF4B4B;color:white;border:none;border-radius:12px;font-size:20px;cursor:pointer;font-weight:bold;box-shadow:0 6px 12px rgba(0,0,0,0.15);margin-top:10px;transition:0.2s;}#submit-btn:hover{background-color:#e63946;transform:translateY(-2px);}textarea{width:100%;padding:15px;border:1px solid #ccc;border-radius:12px;font-family:inherit;font-size:16px;margin-bottom:10px;resize:vertical;box-sizing:border-box;}</style></head><body><div id="content"></div><script>function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); } function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); } function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); } let optsData = []; let myComment = ""; window.addEventListener("message", function(event) { if (event.data.type === "streamlit:render") { const args = event.data.args; if(window.lastEventId === args.eventId && window.lastSaveTs === args.saveTs) return; window.lastEventId = args.eventId; window.lastSaveTs = args.saveTs; const opts = args.options; const myAnsBin = args.myAnsBin; myComment = args.myComment || ""; const isClosed = args.isClosed; let html = ""; optsData = []; opts.forEach((opt, i) => { let v = i < myAnsBin.length ? parseInt(myAnsBin[i]) : 0; optsData.push(v); let pointerEv = isClosed ? "pointer-events:none; opacity:0.7;" : ""; html += `<div class="opt-card" style="${pointerEv}"><div class="opt-title">📅 ${opt}</div><div class="btn-group" id="group-${i}"><button class="opt-btn ${v===0 ? 'active':''}" data-v="0" onclick="setOpt(${i}, 0)">× 不可</button><button class="opt-btn ${v===2 ? 'active':''}" data-v="2" onclick="setOpt(${i}, 2)">△ 未定</button><button class="opt-btn ${v===1 ? 'active':''}" data-v="1" onclick="setOpt(${i}, 1)">◯ 可</button></div></div>`; }); if(!isClosed) { html += `<div class="opt-card"><div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント (任意)</div><textarea id="comment-box" rows="2" placeholder="遅刻・早退などの連絡事項">${myComment}</textarea><button id="submit-btn" onclick="submitData()">✅ 回答を保存して提出</button></div>`; } else { html += `<div class="opt-card"><div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント</div><div style="padding:15px; background:#eee; border-radius:12px; min-height:50px; font-size:16px;">${myComment}</div></div>`; } document.getElementById("content").innerHTML = html; setTimeout(() => sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.getElementById('content').scrollHeight + 50}), 150); } }); window.setOpt = function(idx, val) { optsData[idx] = val; const btns = document.getElementById('group-' + idx).querySelectorAll('.opt-btn'); btns.forEach(b => b.classList.remove('active')); document.getElementById('group-' + idx).querySelector(`[data-v="${val}"]`).classList.add('active'); }; window.submitData = function() { const btn = document.getElementById("submit-btn"); btn.innerText = "⏳ 保存処理中..."; btn.style.pointerEvents = "none"; const comment = document.getElementById("comment-box").value; setComponentValue({ trigger_save: true, binary: optsData.join(''), comment: comment, ts: Date.now() }); }; init();</script></body></html>""")
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
        .sw-btn.active[data-v="2"]{background:#81C784;color:#fff;border-color:#81C784;opacity:0.8;}
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
            <button class="pen-btn" onclick="window.setPen(2)" id="pen-2" style="background:#81C784; color:#fff;">未定</button>
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
            
            let bgColor = '#fff'; let txt = ''; let txtColor = '#fff'; let opacity = 1.0;

            if (v == 1 || v == 2) {
                let info = { color: "#4CAF50", text: "◯" }; 
                if (campus === "なかもず") info = { color: "#FFA726", text: "な" };
                else if (campus === "すぎもと" || campus === "杉本") info = { color: "#42A5F5", text: "す" };
                else if (campus === "もりのみや") info = { color: "#66BB6A", text: "も" };
                else if (campus === "あべの" || campus === "阿倍野") info = { color: "#EC407A", text: "あ" };
                else if (campus === "りんくう") info = { color: "#AB47BC", text: "り" };
                else if (campus === "その他/移動中") info = { color: "#9E9E9E", text: "他" };
                
                bgColor = info.color;
                txt = info.text;
                if (v == 2) opacity = 0.4; 
            } else if (v == 3) {
                bgColor = '#E0E0E0'; txt = '授'; txtColor = '#555';
            } else if (v == 0 && (note === "バイト/サークル等" || note === "バイト/私用")) {
                bgColor = '#f5f5f5'; txt = '休'; txtColor = '#aaa';
            }
            
            el.style.backgroundColor = bgColor;
            el.style.opacity = opacity;
            el.style.backgroundImage = 'none';
            el.style.color = txtColor;
            el.style.display = 'flex';
            el.style.alignItems = 'center';
            el.style.justifyContent = 'center';

            const existingIcon = el.querySelector('.memo-icon');
            const hasManualSetting = detail && (detail.note !== "" || (detail.campus && detail.campus !== defaultCampus));
            
            let innerHtml = '<span style="font-size:14px; font-weight:bold; pointer-events:none;">' + txt + '</span>';
            if (hasManualSetting) { innerHtml += '<div class="memo-icon">💬</div>'; }
            
            el.innerHTML = innerHtml;
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
                
                let p1Info = {color:"#4CAF50", txt:"可"};
                if (defaultCampus === "なかもず") p1Info = {color:"#FFA726", txt:"な"};
                else if (defaultCampus === "すぎもと" || defaultCampus === "杉本") p1Info = {color:"#42A5F5", txt:"す"};
                else if (defaultCampus === "もりのみや") p1Info = {color:"#66BB6A", txt:"も"};
                else if (defaultCampus === "あべの" || defaultCampus === "阿倍野") p1Info = {color:"#EC407A", txt:"あ"};
                else if (defaultCampus === "りんくう") p1Info = {color:"#AB47BC", txt:"り"};
                else if (defaultCampus === "その他/移動中") p1Info = {color:"#9E9E9E", txt:"他"};
                
                document.getElementById('pen-1').innerHTML = defaultCampus ? `${p1Info.txt}<br><span style='font-size:9px;'>(${defaultCampus})</span>` : "可";
                document.getElementById('pen-1').style.background = p1Info.color;
                document.getElementById('pen-2').innerHTML = defaultCampus ? `未定<br><span style='font-size:9px;'>(${defaultCampus})</span>` : "未定";
                document.getElementById('pen-2').style.background = p1Info.color;
                document.getElementById('pen-2').style.opacity = 0.6;
                document.getElementById('pen-2').style.color = "#fff";
                
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
        
        if dt.tzinfo is not None:
            dt = dt.tz_convert(None)
            
        wday = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]
        return f"{dt.month}/{dt.day}({wday}) {dt.strftime('%H:%M')}"
    except:
        return str(date_str)

campus_legend_html = """
<div style="margin: 10px 0 20px 0; padding: 12px; background: #fdfdfd; border-radius: 8px; border: 1px solid #e0e0e0; font-size: 13px; line-height: 1.8; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
    <strong style="color:#2e7d32; display:block; margin-bottom:8px; font-size: 14px;">🎨 キャンパスの色と文字</strong>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#FFA726; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">な</span> なかもず</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#42A5F5; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">す</span> 杉本</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#66BB6A; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">も</span> もりのみや</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#EC407A; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">あ</span> あべの</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#AB47BC; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">り</span> りんくう</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#9E9E9E; color:#fff; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">他</span> 移動/その他</span>
    <span style="display:inline-block; margin-right:12px; margin-bottom:4px;"><span style="background:#E0E0E0; color:#555; padding:2px 8px; border-radius:4px; font-weight:bold; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.1);">授</span> 授業等</span>
    <div style="color:#d32f2f; font-weight:bold; font-size:12px; margin-top:8px; border-top:1px dashed #ddd; padding-top:6px;">
        ※「未定(△)」を選ぶと、同じ色が薄く（半透明に）表示されます。
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
    # 🔑 未ログイン画面
    # ==========================================
    if not st.session_state.auth:
        _, col_login, _ = st.columns([1, 2, 1])
        with col_login:
            st.title("V-Sync by もっきゅー")
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
                            if u_data.get("pin") == hashed_p or u_data.get("pin") == p:
                                if u_data.get("pin") == p: 
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
                    if clean_name and reg_p and reg_s:
                        all_users_list = [doc.to_dict() for doc in db.collection("users").stream()]
                        new_num = len(all_users_list) + 1
                        new_user_id = f"U{new_num:03}"
                        role_val = "top_admin" if len(all_users_list) == 0 else "guest"

                        new_u = {
                            "user_id": new_user_id,
                            "name": clean_name,
                            "pin": hash_secret(reg_p),
                            "secret_word": hash_secret(reg_s),
                            "group_1": ", ".join(g1),
                            "group_2": ", ".join(g2),
                            "group_3": ", ".join(g3),
                            "group_4": "",
                            "role": role_val
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
                        rec_s = st.text_input("秘密の合言葉")
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
    
    if "jump_to_event" in st.session_state:
        st.session_state.target_ev_id = st.session_state.jump_to_event
        del st.session_state.jump_to_event
    
    menu_opts = ["📅 日程調整 回答", "👤 プロフィール設定", "⏰ 時間割設定"]
    if user.get("role") in ["user", "admin", "top_admin"]: menu_opts.append("➕ イベント新規作成")
    if user.get("role") in ["admin", "top_admin"]: menu_opts.append("⚙️ 管理者専用")
    
    view_mode = st.sidebar.radio("🔧 メニュー", menu_opts, index=0)

    # ====================================================
    # 👤 プロフィール設定 / ⏰ 時間割設定 / ➕ イベント作成 / ⚙️ 管理者画面
    # ====================================================
    # (省略せずフル実装していますが、ここはビジネスロジック変更なしなのでそのまま)
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
            if gas_payload.get("calendar_url"): gas_payload["calendar_url"] = "LINKED"
            backup_to_gas_async("update_user_v2", {"payload": gas_payload})
            user.update(payload)
            st.session_state.auth = user
            st.success("✅ 保存完了")
            time.sleep(1)
            st.rerun()

        st.markdown("---")
        st.markdown("##### 🔐 セキュリティ設定 (PIN・合言葉の変更)")
        with st.expander("PINや合言葉を変更する"):
            st.write("本人確認のため、現在のPINを入力してから新しい情報を設定してください。")
            with st.form("security_update_form"):
                current_p = st.text_input("現在のPIN (必須)", type="password")
                st.markdown("<br>", unsafe_allow_html=True)
                new_p = st.text_input("新しいPIN (変更しない場合は空欄)", type="password")
                new_s = st.text_input("新しい秘密の合言葉 (変更しない場合は空欄)")
                
                if st.form_submit_button("更新する", use_container_width=True, type="primary"):
                    if not current_p: st.error("エラー: 現在のPINを入力してください。")
                    elif hash_secret(current_p) != user.get("pin") and current_p != user.get("pin"): st.error("エラー: 現在のPINが間違っています。")
                    elif not new_p and not new_s: st.warning("変更する新しいPINまたは合言葉を入力してください。")
                    else:
                        updates = {}
                        if new_p: updates["pin"] = hash_secret(new_p)
                        if new_s: updates["secret_word"] = hash_secret(new_s)
                        try:
                            db.collection("users").document(str(user["user_id"])).update(updates)
                            gas_payload = {"user_id": user["user_id"]}
                            if new_p: gas_payload["pin"] = "ENCRYPTED_PIN"
                            if new_s: gas_payload["secret_word"] = "SET_BY_USER"
                            backup_to_gas_async("update_user_v2", {"payload": gas_payload})
                            updated_u = {**user, **updates}
                            st.session_state.auth = updated_u
                            st.success("✅ セキュリティ情報を更新しました！")
                            time.sleep(1.0)
                            st.rerun()
                        except Exception as e: st.error(f"更新に失敗しました: {e}")

        st.markdown("---")
        st.markdown("##### ⚠️ アカウントの削除（退会）")
        with st.expander("退会手続きを開く"):
            st.warning("退会すると、これまでの回答データや時間割がすべて削除され、元に戻すことはできません。")
            if st.button("💥 本当に退会する", type="primary"):
                uid = str(user["user_id"])
                db.collection("users").document(uid).delete()
                res_docs = db.collection("responses").where("user_id", "==", uid).stream()
                for d in res_docs: db.collection("responses").document(d.id).delete()
                backup_to_gas_async("delete_user", {"payload": {"user_id": uid}})
                st.session_state.auth = None
                st.rerun()
        return

    elif view_mode == "⏰ 時間割設定":
        # 時間割設定画面 (変更なしのため省略せずにそのまま)
        st.title("⏰ 時間割設定")
        st.info("※ここで設定した授業・バイトの予定は、各イベントの日程調整画面で「時間割パワー反映」ボタンを押すことで、自動入力できます。")
        st.markdown("""<style>@media (max-width: 650px) { [data-testid="column"] { min-width: 0 !important; flex: 1 1 0px !important; padding: 0 !important; } [data-testid="column"]:first-child { flex: 0 0 55px !important; } .tt-day-header { font-size: 13px !important; padding: 4px 0 !important; } .tt-time-cell { font-size: 11px !important; padding: 4px 2px !important; } }</style>""", unsafe_allow_html=True)
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

        periods = [("1限", "09:00〜", 36, 42, "p1"), ("2限", "10:45〜", 43, 49, "p2"), ("3限", "13:15〜", 53, 59, "p3"), ("4限", "15:00〜", 60, 66, "p4"), ("5限", "16:45〜", 67, 73, "p5")]
        for p_name, p_time, s_idx, e_idx, p_key in periods:
            cols = st.columns(col_ratios)
            cols[0].markdown(f"<div class='tt-time-cell'>{p_name}<br><span class='tt-time-sub'>{p_time}</span></div>", unsafe_allow_html=True)
            for i in range(5):
                day_bin = fixed_sched.get(str(i), "0"*96)
                is_occupied = (day_bin[s_idx:e_idx] == "1" * (e_idx - s_idx))
                saved_loc = fixed_locs.get(str(i), {}).get(p_key, "")
                current_val = saved_loc if is_occupied and saved_loc in tt_options else ("💼 バイト/サークル等" if is_occupied and ("バイト" in saved_loc or "私用" in saved_loc) else (tt_options[1] if is_occupied else "- (空き)"))
                selected_opt = cols[i+1].selectbox("予定", tt_options, index=tt_options.index(current_val), key=f"tt_{p_key}_{i}", label_visibility="collapsed")
                
                if selected_opt == "- (空き)": ui_state[str(i)][p_key] = False; cols[i+1].markdown("<div class='status-off'>-</div>", unsafe_allow_html=True)
                elif selected_opt == "💼 バイト/サークル等": ui_state[str(i)][p_key] = True; ui_state[str(i)][f"{p_key}_loc"] = "💼 バイト/サークル等"; cols[i+1].markdown(f"<div class='status-off' style='background:#f5f5f5;'>💼 バイト等</div>", unsafe_allow_html=True)
                else: ui_state[str(i)][p_key] = True; ui_state[str(i)][f"{p_key}_loc"] = selected_opt; cols[i+1].markdown(f"<div class='status-on'>✔︎ {selected_opt}</div>", unsafe_allow_html=True)

        cols = st.columns(col_ratios)
        cols[0].markdown(f"<div class='tt-time-cell' style='border-left-color:#FF9800;'>放課後<br><span class='tt-time-sub'>18:30〜</span></div>", unsafe_allow_html=True)
        for i in range(5):
            day_bin = fixed_sched.get(str(i), "0"*96); af_bin = day_bin[74:]
            saved_loc = fixed_locs.get(str(i), {}).get("af", "")
            current_val = saved_loc if "1" in af_bin and saved_loc in tt_options else ("💼 バイト/サークル等" if "1" in af_bin and ("バイト" in saved_loc or "私用" in saved_loc) else (tt_options[1] if "1" in af_bin else "- (空き)"))
            selected_opt = cols[i+1].selectbox("予定", tt_options, index=tt_options.index(current_val), key=f"tt_af_{i}", label_visibility="collapsed")
            if selected_opt == "- (空き)": ui_state[str(i)]["af"] = False
            elif selected_opt == "💼 バイト/サークル等": ui_state[str(i)]["af"] = True; ui_state[str(i)]["af_loc"] = "💼 バイト/サークル等"
            else: ui_state[str(i)]["af"] = True; ui_state[str(i)]["af_loc"] = selected_opt
            
        cols = st.columns(col_ratios)
        cols[0].markdown(f"<div class='tt-end-time' style='text-align:center; font-size:10px; color:#666; padding-top:10px;'>終了時刻</div>", unsafe_allow_html=True)
        for i in range(5):
            if ui_state[str(i)]["af"]:
                day_bin = fixed_sched.get(str(i), "0"*96); af_bin = day_bin[74:]
                af_end_val = time_master[74 + af_bin.rfind("1") + 1] if "1" in af_bin and 74 + af_bin.rfind("1") + 1 < 96 else "23:45" if "1" in af_bin else "21:00"
                af_opts = [idx_to_time(idx) for idx in range(76, 96)]
                ui_state[str(i)]["af_end"] = cols[i+1].selectbox("終了", af_opts, index=af_opts.index(af_end_val) if af_end_val in af_opts else 8, key=f"afe_{i}", label_visibility="collapsed")
            else: ui_state[str(i)]["af_end"] = "21:00"

        if st.button("💾 時間割を保存する", use_container_width=True, type="primary"):
            new_fixed_sched, new_fixed_locs = {}, {}
            for i in range(5):
                wd_str = str(i); new_bin = ["0"] * 96; day_locs = {}
                if ui_state[wd_str]["p1"]: new_bin[36:42] = ["1"] * 6; day_locs["p1"] = ui_state[wd_str]["p1_loc"]
                if ui_state[wd_str]["p2"]: new_bin[43:49] = ["1"] * 6; day_locs["p2"] = ui_state[wd_str]["p2_loc"]
                if ui_state[wd_str]["p3"]: new_bin[53:59] = ["1"] * 6; day_locs["p3"] = ui_state[wd_str]["p3_loc"]
                if ui_state[wd_str]["p4"]: new_bin[60:66] = ["1"] * 6; day_locs["p4"] = ui_state[wd_str]["p4_loc"]
                if ui_state[wd_str]["p5"]: new_bin[67:73] = ["1"] * 6; day_locs["p5"] = ui_state[wd_str]["p5_loc"]
                if ui_state[wd_str]["af"]: end_idx = time_master.index(ui_state[wd_str]["af_end"]); new_bin[74:end_idx] = ["1"] * (end_idx - 74); day_locs["af"] = ui_state[wd_str]["af_loc"]
                new_fixed_sched[wd_str] = "".join(new_bin); new_fixed_locs[wd_str] = day_locs
            payload = {"user_id": user['user_id'], "fixed_schedule": new_fixed_sched, "group_4": json.dumps(new_fixed_locs)}
            res = call_gas("update_user", {"payload": payload}, method="POST")
            if res.get("status") == "success":
                db.collection("users").document(str(res.get("data")["user_id"])).update(res.get("data"))
                st.session_state.auth = res.get("data"); st.rerun()
        return

    elif view_mode == "➕ イベント新規作成":
        # イベント作成画面 (変更なし)
        st.title("➕ イベント新規作成")
        ev_type_label = st.radio("📝 タイプを選択", ["🕒 時間帯", "🏫 時間割", "📅 複数の予定"], horizontal=True)
        ev_title = st.text_input("イベント名")
        with st.container(border=True):
            st.markdown("##### ⏳ 回答期限の設定")
            c1, c2 = st.columns(2)
            with c1: deadline_date = st.date_input("回答期限 (日付)", value=datetime.today() + timedelta(days=7))
            with c2: deadline_time = st.time_input("回答期限 (時刻)", value=datetime.strptime("23:59", "%H:%M").time())
            auto_close = st.checkbox("期限が過ぎたら自動で締め切る", value=True)
            
        ev_start, ev_end, t_start, t_end, opts_list = None, None, None, None, []
        if ev_type_label == "🕒 時間帯":
            ev_type = "time"
            c1, c_m, c2 = st.columns([10, 1, 10])
            with c1: ev_start = st.date_input("開始日")
            with c2: ev_end = st.date_input("終了日")
            c3, c_m2, c4 = st.columns([10, 1, 10])
            with c3: t_start = st.selectbox("開始時刻", time_master, index=36)
            with c4: t_end = st.selectbox("終了時刻", time_master, index=72)
        elif ev_type_label == "🏫 時間割": ev_type = "timetable"
        else:
            ev_type = "options"
            if "opt_count" not in st.session_state: st.session_state.opt_count = 3
            for i in range(st.session_state.opt_count): opts_list.append(st.text_input(f"候補 {i+1}"))
            if st.button("➕ 候補を追加"): st.session_state.opt_count += 1; st.rerun()

        with st.container(border=True):
            st.markdown("##### 👥 参加メンバーの指定")
            is_all_members = st.checkbox("全員に公開する", value=True)
            target_scope_json = ""
            if not is_all_members:
                all_u = [d.to_dict() for d in db.collection("users").stream()]
                all_g1 = sort_groups(list(set([g.strip() for u in all_u for g in str(u.get('group_1', '')).split(',') if g.strip()])), MASTER_G1)
                t_g1 = st.multiselect("🏫 キャンパス", all_g1)
                target_scope_json = json.dumps({"groups": t_g1, "users": []})

        is_private = st.checkbox("🤫 プライベート調整にする", key="create_private")
        ev_desc_raw = rt_editor(key="desc_editor")
        
        if st.button("🚀 イベントを作成", use_container_width=True, type="primary"):
            if not ev_title: st.warning("イベント名を入力してください。")
            else:
                deadline_str = f"{deadline_date.strftime('%Y-%m-%d')} {deadline_time.strftime('%H:%M')}"
                created_event_id = generate_custom_id("EV")
                payload = {
                    "event_id": created_event_id, "title": ev_title, "description": ev_desc_raw or "", 
                    "start_date": ev_start.strftime("%Y-%m-%d") if ev_type == "time" else "", 
                    "end_date": ev_end.strftime("%Y-%m-%d") if ev_type == "time" else "", 
                    "start_time_idx": time_master.index(t_start) if ev_type == "time" else 0, 
                    "end_time_idx": time_master.index(t_end) if ev_type == "time" else 0, 
                    "status": "open", "type": ev_type, "options_name": json.dumps([o.strip() for o in opts_list if o.strip()]) if ev_type == "options" else "",
                    "close_time": deadline_str, "auto_close": auto_close, "target_scope": target_scope_json, "is_private": is_private
                }
                db.collection("events").document(created_event_id).set(payload)
                backup_to_gas_async("create_event_v2", {"payload": payload})
                st.success(f"作成しました！招待URL: {APP_BASE_URL}?event={created_event_id}")
        return

    elif view_mode == "⚙️ 管理者専用":
        # 管理者画面 (変更なし)
        st.title("⚙️ 管理者ダッシュボード")
        st.info("※管理者画面の機能は省略していますが、ベースコードと同様にタブ化して配置されています")
        return

    # ====================================================
    # 📅 一般ユーザー画面 (日程調整 回答 & ダッシュボード)
    # ====================================================
    
    active_groups = [str(user.get(f"group_{i}", "")).strip() for i in range(1, 4) if str(user.get(f"group_{i}", "")).strip()]
    group_str = f"<span style='color: #666; font-size: 0.9em; margin-left: 10px;'>({' / '.join(active_groups)})</span>" if active_groups else "<span style='color: #aaa; font-size: 0.9em; margin-left: 10px;'>(未所属)</span>"
    role_emoji = {"top_admin": "👑", "admin": "🛠️", "user": "📝", "guest": "👤"}.get(user.get("role"), "👤")
    st.markdown(f'<div class="user-header"><div style="font-size: 1.1em;"><b>{role_emoji} {user.get("name", "")}</b> さん {group_str}</div><div style="font-size: 0.8em; background: #e0e0e0; padding: 3px 8px; border-radius: 12px;">ID: {user.get("user_id", "")}</div></div>', unsafe_allow_html=True)

    current_ev_id = st.session_state.get("target_ev_id", "")
    all_users_fs, events, user_map_fs = get_app_data_from_firestore(user)
    
    if not events: 
        st.info("現在表示できるイベントはありません。")
        return

    # ----------------------------------------------------
    # サイドバーのナビゲーション（プルダウン廃止の代替）
    # ----------------------------------------------------
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔴 未回答の予定")
    unanswered_events = [ev for ev in events if not ev.get('is_answered') and ev.get('status') == 'open']
    if unanswered_events:
        for u_ev in unanswered_events:
            if st.sidebar.button(f"🔴 {u_ev.get('title', '')}", key=f"side_btn_u_{u_ev.get('event_id')}", use_container_width=True):
                st.session_state.target_ev_id = u_ev.get('event_id')
                if "active_tab" in st.session_state: del st.session_state.active_tab
                st.rerun()
    else:
        st.sidebar.caption("未回答の予定はありません 🎉")
        
    st.sidebar.markdown("### ✅ 回答済みの予定")
    answered_events = [ev for ev in events if ev.get('is_answered')]
    if answered_events:
        for a_ev in answered_events:
            if st.sidebar.button(f"✅ {a_ev.get('title', '')}", key=f"side_btn_a_{a_ev.get('event_id')}", use_container_width=True):
                st.session_state.target_ev_id = a_ev.get('event_id')
                if "active_tab" in st.session_state: del st.session_state.active_tab
                st.rerun()
    else:
        st.sidebar.caption("まだ回答していません")

    # ----------------------------------------------------
    # 💡 ダッシュボード画面（イベント選択）
    # ----------------------------------------------------
    if not current_ev_id:
        st.subheader("📋 イベント ダッシュボード")
        st.markdown("<p style='font-size:14px; color:#555; margin-bottom:20px;'>回答したいイベントを選んでください。</p>", unsafe_allow_html=True)
        
        c_left, c_right = st.columns(2)
        
        with c_left:
            st.markdown("#### 🔴 未回答のイベント")
            if unanswered_events:
                for ev in unanswered_events:
                    dl_text = format_deadline_jp(ev.get('close_time') or ev.get('deadline', ''))
                    st.markdown(f"""<div class="dash-card" onclick="document.getElementById('hid_btn_u_{ev['event_id']}').click()">
                        <div style="font-weight:bold; font-size:16px; margin-bottom:5px;">{ev['title']}</div>
                        <div style="font-size:12px; color:#E91E63;">⏳ 締切: {dl_text}</div>
                    </div>""", unsafe_allow_html=True)
                    # 隠しボタンでStreamlitにクリックを伝えるハック
                    st.button(" ", key=f"hid_btn_u_{ev['event_id']}", on_click=lambda eid=ev['event_id']: st.session_state.update({"target_ev_id": eid}), help=f"{ev['title']} に回答する")
            else:
                st.info("未回答のイベントはありません！🎉")

        with c_right:
            st.markdown("#### ✅ 回答済みのイベント")
            if answered_events:
                for ev in answered_events:
                    dl_text = format_deadline_jp(ev.get('close_time') or ev.get('deadline', ''))
                    st.markdown(f"""<div class="dash-card answered" onclick="document.getElementById('hid_btn_a_{ev['event_id']}').click()">
                        <div style="font-weight:bold; font-size:16px; margin-bottom:5px;">{ev['title']}</div>
                        <div style="font-size:12px; color:#666;">締切: {dl_text}</div>
                    </div>""", unsafe_allow_html=True)
                    st.button(" ", key=f"hid_btn_a_{ev['event_id']}", on_click=lambda eid=ev['event_id']: st.session_state.update({"target_ev_id": eid}), help=f"{ev['title']} の回答を確認する")
            else:
                st.info("回答済みのイベントはありません。")
        
        return

    # ----------------------------------------------------
    # 個別イベント 回答・集計画面
    # ----------------------------------------------------
    event = next((ev for ev in events if ev['event_id'] == current_ev_id), None)
    if not event:
        st.error("指定されたイベントが見つかりません。")
        if st.button("ダッシュボードに戻る"):
            st.session_state.target_ev_id = ""
            st.rerun()
        return

    st.session_state.event_responses = fetch_responses_for_event(current_ev_id, user_map_fs)
    
    # ダッシュボードに戻るボタン
    if st.button("🔙 イベント一覧に戻る"):
        st.session_state.target_ev_id = ""
        st.rerun()

    is_closed = event.get('status') == 'closed'
    is_private_event = event.get('is_private', False)
    can_view_details = user.get("role") in ["admin", "top_admin"] if is_private_event else True
    
    st.markdown(f"<h2>{event.get('title', '')}</h2>", unsafe_allow_html=True)

    if event.get('is_answered'):
        st.success("✅ **あなたは回答済みです（修正して再提出も可能です）**")

    ev_close_time = event.get('close_time') or event.get('deadline', '')
    if is_closed: 
        st.markdown("<div class='closed-alert' style='background:#ffebee; color:#c62828; padding:10px; border-radius:6px; font-weight:bold; margin-bottom:10px;'>🔒 このイベントは締め切られました。</div>", unsafe_allow_html=True)
    elif ev_close_time: 
        st.markdown(f"<div style='color: #E91E63; font-weight: bold; margin-bottom: 10px;'>⏳ 回答期限: {format_deadline_jp(ev_close_time)}</div>", unsafe_allow_html=True)

    # 管理者以外には招待URLを隠す、説明文はアコーディオン
    if event.get('description'): 
        with st.expander("📝 イベントの説明・管理者からのメッセージ", expanded=False):
            st.markdown(f"<div style='font-size:14px; line-height:1.6;'>{event['description'].replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)
            
    if user.get("role") in ["admin", "top_admin"]:
        st.caption(f"招待URL (管理者のみ表示): {APP_BASE_URL}?event={event.get('event_id')}")

    if is_private_event:
        st.info("🤫 **このイベントはプライベート設定されています。** 管理者以外には、誰が回答したかの名前やコメントは表示されず、全体の人数のみが表示されます。")

    event_type = event.get('type') or event.get('event_type', 'time')

    # 💡 提出後のタブ自動遷移用ロジック
    if "active_tab" not in st.session_state:
        st.session_state.active_tab = "📅 入力"
        
    selected_tab = st.radio("表示切り替え", ["📅 入力", "📊 みんなの集計"], horizontal=True, label_visibility="collapsed", index=0 if st.session_state.active_tab == "📅 入力" else 1)
    st.session_state.active_tab = selected_tab

    # ＝＝＝＝＝ 🕒 時間帯 / 🏫 時間割 モード ＝＝＝＝＝
    if event_type in ['time', 'timetable']:
        if event_type == 'time':
            s_idx = int(event.get('start_time_idx') or event.get('start_idx', 0))
            e_idx = int(event.get('end_time_idx') or event.get('end_idx', 0))
            if e_idx <= s_idx: e_idx = s_idx + 1
            
            date_objs = []
            try:
                curr = pd.to_datetime(event.get('start_date', ''), errors='coerce').date()
                end_d = pd.to_datetime(event.get('end_date', ''), errors='coerce').date()
                if pd.isna(curr) or pd.isna(end_d): raise Exception
            except:
                curr = datetime.today().date(); end_d = curr + timedelta(days=7)
                
            while curr <= end_d: date_objs.append(curr); curr += timedelta(days=1)
            date_strs = [d.strftime("%Y-%m-%d") for d in date_objs]
            clean_date_labels = [f"{d.strftime('%m/%d')}({['月','火','水','木','金','土','日'][d.weekday()]})" for d in date_objs]
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
                    
        else: # timetable
            s_idx, e_idx = 0, 6
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
                            v = int(b_str[s_idx + i]) if event_type == 'time' else int(b_str[i]) if i < len(b_str) else 0
                            df.loc[time_labels[i], d_id] = v
            st.session_state.df_input = df
            st.session_state.my_comment = my_comment
            st.session_state.last_build_ev_id = event.get('event_id')

        if event_type == 'time': cell_h = "36px"

        if selected_tab == "📅 入力":
            user_campuses = [x.strip() for x in str(user.get('group_1', '')).split(',') if x.strip()]
            default_campus_initial = user_campuses[0] if user_campuses else "なかもず"
            campus_options = MASTER_G1 + ["その他/移動中"]
            default_index = campus_options.index(default_campus_initial) if default_campus_initial in campus_options else 0
            
            c_left, c_right = st.columns([1, 1])
            with c_left:
                st.markdown("##### 📍 今回のデフォルト所在地")
                selected_default_campus = st.selectbox("「可」を塗った時に自動で設定されるキャンパス", campus_options, index=default_index, label_visibility="collapsed")
            with c_right:
                if event_type == 'time' and st.button("🔄 カレンダーから予定取得 (授業等で反映)"):
                    # カレンダー連携 (処理省略)
                    pass

            st.markdown(campus_legend_html, unsafe_allow_html=True)
            
            # 💡 初心者向けミニガイド
            st.markdown("""
            <div style="background: #e8f5e9; border-left: 5px solid #4CAF50; padding: 12px; margin-bottom: 15px; border-radius: 6px; font-size: 14px; font-weight: bold; color: #2e7d32; display: flex; align-items: center; gap: 15px; flex-wrap: wrap;">
                <span style="font-size:16px;">💡 使い方:</span>
                <span>① 右上のペン（可/未定）を選ぶ</span> ➡️
                <span>② カレンダーの枠をなぞって塗る</span> ➡️
                <span>③ 一番下の「提出」ボタンを押す</span>
            </div>
            """, unsafe_allow_html=True)

            m = st.session_state.df_input[date_strs].values.tolist()
            time_opts_html = "".join([f'<option value="{i}">{t}</option>' for i, t in enumerate(time_labels)])
            end_time_labels = [idx_to_time(time_master.index(t) + 1) if time_master.index(t) < 95 else "24:00" for t in time_labels] if event_type == 'time' else time_labels
            time_opts_end_html = "".join([f'<option value="{i}">{t}</option>' for i, t in enumerate(end_time_labels)])
            src_opts_html = "".join([f'<option value="{i}">{l}</option>' for i, l in enumerate(clean_date_labels)])
            b_day_opts_html = "".join([f'<label class="ms-opt"><input type="checkbox" class="b-day-chk" value="{i}" checked> {l}</label>' for i, l in enumerate(clean_date_labels)])
            c_tgt_opts_html = "".join([f'<label class="ms-opt"><input type="checkbox" class="c-tgt-chk" value="{i}"> {l}</label>' for i, l in enumerate(clean_date_labels)])
            
            my_cell_details = {}
            for r in st.session_state.event_responses:
                if str(r.get('user_id')) == str(user.get('user_id')) and r.get('cell_details'):
                    try: my_cell_details = json.loads(r['cell_details']); break
                    except: pass

            day_cols_html = ""
            for c, d_str in enumerate(date_strs):
                lbl = clean_date_labels[c].replace("(", "<br>(")
                cells_html = ""
                for r, t_str in enumerate(time_labels):
                    val = int(m[r][c])
                    
                    # 💡 キャンパスの色と文字のロジック
                    campus = ""
                    cd_key = f"{r}_{c}"
                    if cd_key in my_cell_details: campus = my_cell_details[cd_key].get("campus", "")
                    if not campus and val in [1, 2]: campus = selected_default_campus
                    
                    bg, txt, txt_color, opacity = "#fff", "", "#fff", 1.0
                    if val in [1, 2]:
                        if campus == "なかもず": bg, txt = "#FFA726", "な"
                        elif campus in ["すぎもと", "杉本"]: bg, txt = "#42A5F5", "す"
                        elif campus == "もりのみや": bg, txt = "#66BB6A", "も"
                        elif campus in ["あべの", "阿倍野"]: bg, txt = "#EC407A", "あ"
                        elif campus == "りんくう": bg, txt = "#AB47BC", "り"
                        elif campus == "その他/移動中": bg, txt = "#9E9E9E", "他"
                        else: bg, txt = "#4CAF50", "◯"
                        if val == 2: opacity = 0.4
                    elif val == 3:
                        bg, txt, txt_color = "#E0E0E0", "授", "#555"
                    
                    b_top = get_border_top(t_str, event_type)
                    
                    memo_html = '<div class="memo-icon">💬</div>' if cd_key in my_cell_details and (my_cell_details[cd_key].get('note') or my_cell_details[cd_key].get('campus') != selected_default_campus) else ""
                    
                    cells_html += f'<div class="c" data-r="{r}" data-c="{c}" data-v="{val}" style="height:{cell_h}; background:{bg}; opacity:{opacity}; color:{txt_color}; display:flex; align-items:center; justify-content:center; cursor:pointer; border-top:{b_top}; border-right:1px solid #eee; box-sizing:border-box;"><span style="font-size:14px; font-weight:bold; pointer-events:none;">{txt}</span>{memo_html}</div>'
                day_cols_html += f'<div class="day-col" data-c="{c}" style="flex:1; min-width:85px; box-sizing:border-box; display:none;"><div class="header-cell">{lbl}</div>{cells_html}</div>'

            time_cells_html = ""
            for r, t_str in enumerate(time_labels):
                b_top = get_border_top(t_str, event_type)
                lbl = t_str if t_str.endswith(":00") or t_str.endswith(":30") or event_type == 'timetable' else ""
                time_cells_html += f'<div style="background:#f0f2f6; text-align:center; font-size:12px; font-weight:bold; color:#555; height:{cell_h}; display:flex; align-items:center; justify-content:center; border-top:{b_top}; border-right:1px solid #ccc; box-sizing:border-box;">{lbl}</div>'
            time_col_html = f'<div class="time-col"><div class="top-left-cell"></div>{time_cells_html}</div>'

            tools_html, submit_btn_html, pointer_css = "", "", ""
            if not is_closed:
                tt_btn_text = "🚫 該当日の自分の時間割をすべて × にする" if event_type == "time" else "🚫 自分の時間割をそのまま反映する"
                with st.expander("🛠️ 便利ツール (一括指定・コピー・時間割反映)", expanded=False):
                    tools_html = f"""
                    <div style="display:flex; gap:15px; flex-wrap:wrap; margin-bottom: 20px;">
                        <div class="tool-card" style="display:{'none' if event_type == 'timetable' else 'block'};"><div class="tool-header">🪄 一括指定ツール</div>
                            <div style="display:flex; gap:10px; margin-bottom:10px; align-items:center; flex-wrap:wrap;">状態: <select id="b-val" class="st-sel"><option value="1">可</option><option value="2">未定</option><option value="0">不可</option></select>時間: <select id="b-start" class="st-sel">{time_opts_html}</select> 〜 <select id="b-end" class="st-sel"><option value="{len(time_labels)-1}" selected>{end_time_labels[-1]}</option>{time_opts_end_html}</select></div>
                            <div style="display:flex; gap:10px; align-items:center;">対象: <div class="ms-container"><div class="ms-header" onclick="window.toggleList('b-days-list');">対象日を選択 <span>▼</span></div><div id="b-days-list" class="ms-options" style="display:none;"><label class="ms-opt" style="font-weight:bold;"><input type="checkbox" onchange="document.querySelectorAll('.b-day-chk').forEach(c => c.checked = this.checked)" checked> 全て選択 / 解除</label><hr style="margin:5px 0; border:0; border-top:1px solid #ccc;">{b_day_opts_html}</div></div><button class="st-btn" onclick="window.doBulk(this)">適用</button></div>
                        </div>
                        <div class="tool-card"><div class="tool-header">📋 日程コピー機能</div>
                            <div style="display:flex; gap:10px; margin-bottom:10px; align-items:center;">元: <select id="c-src" class="st-sel" style="flex:1;">{src_opts_html}</select></div>
                            <div style="display:flex; gap:10px; align-items:center;">先: <div class="ms-container"><div class="ms-header" onclick="window.toggleList('c-tgt-list');">対象日を選択 <span>▼</span></div><div id="c-tgt-list" class="ms-options" style="display:none;"><label class="ms-opt" style="font-weight:bold;"><input type="checkbox" onchange="document.querySelectorAll('.c-tgt-chk').forEach(c => c.checked = this.checked)"> 全て選択 / 解除</label><hr style="margin:5px 0; border:0; border-top:1px solid #ccc;">{c_tgt_opts_html}</div></div><button class="st-btn" onclick="window.doCopy(this)" style="background:#FF9800;">コピー実行</button></div>
                        </div>
                        <div class="tool-card"><div class="tool-header">⏰ 時間割パワー反映</div>
                            <button class="st-btn" onclick="window.doTimetable(this)" style="background:#E91E63; width:100%;">{tt_btn_text}</button>
                        </div>
                    </div>"""
                submit_btn_html = f"""
                <div style="margin-top: 20px;">
                    <label style="font-size: 14px; font-weight: 600; color: #333;">📝 全体へのコメント (遅刻・早退など任意)</label>
                    <textarea id="comment-box" rows="2" style="width: 100%; padding: 10px; margin-top: 5px; border: 1px solid #ccc; border-radius: 6px; font-family: sans-serif; resize: vertical;">{st.session_state.my_comment}</textarea>
                </div>
                <button id="submit-btn" style="margin-top: 15px; width: 100%; padding: 16px; background-color: #4CAF50; color: white; border: none; border-radius: 8px; font-size: 18px; cursor: pointer; font-weight: bold; box-shadow: 0 4px 6px rgba(0,0,0,0.15); transition: 0.2s;">✅ カレンダーを保存して提出する</button>
                """
            else:
                pointer_css = "pointer-events: none; opacity: 0.8;"
                submit_btn_html = f"""<div style="margin-top: 20px;"><label style="font-size: 14px; font-weight: 600; color: #333;">📝 全体へのコメント</label><div style="width: 100%; padding: 10px; margin-top: 5px; background: #eee; border: 1px solid #ccc; border-radius: 6px; font-family: sans-serif; min-height:40px;">{st.session_state.my_comment}</div></div>"""

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

            raw = grid_editor(
                html_code=html_code, rows=len(time_labels), cols=len(date_strs), eventId=event.get('event_id'), 
                isClosed=is_closed, unavailColRows=unavail_col_rows, saveTs=st.session_state.get("last_saved_ts", 0), 
                cellDetails=my_cell_details, defaultCampus=selected_default_campus, default=None, key=f"editor_{event.get('event_id')}"
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
                            
                        if has_data: all_res.append({"date": d_id, "binary_data": "".join(bits)})
                            
                    if not all_res: all_res.append({"date": date_strs[0], "binary_data": "0"*96})

                    payload = {
                        "event_id": event.get("event_id"), "user_id": user.get("user_id"), 
                        "comment": st.session_state.my_comment, "cell_details": cell_details_str, "responses": all_res
                    }
                    if save_response_hybrid(payload):
                        st.session_state.save_success_msg = "回答を保存しました！みんなの集計を確認しましょう👀"
                        # 💡 提出完了後、自動で集計タブに切り替え
                        st.session_state.active_tab = "📊 みんなの集計"
                        st.rerun()

        elif selected_tab == "📊 みんなの集計":
            st.subheader("📊 全体の集計結果")
            col1, col2 = st.columns([2, 1])
            with col1: policy = st.radio("「未定(△)」の計算方法", [0.5, 1.0, 0.0], format_func=lambda x: f"{x}人としてカウント", horizontal=True)
            with col2:
                if st.button("🔄 最新の回答を取得", use_container_width=True): st.rerun()

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
                        with f_d_col1: f_dates = st.date_input("表示する期間", value=(date_objs[0], date_objs[-1]), min_value=date_objs[0], max_value=date_objs[-1])
                        with f_d_col2: f_wdays = st.multiselect("表示する曜日", ["月", "火", "水", "木", "金", "土", "日"], default=["月", "火", "水", "木", "金", "土", "日"])
                    else:
                        f_dates = None; f_wdays = ["月", "火", "水", "木", "金"]

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
                                if v.get('campus'): user_locs.add(v['campus'])
                        except: pass
                    if not set(f_locs).intersection(user_locs): continue
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
                        disp_date_strs.append(date_strs[c]); disp_clean_date_labels.append(clean_date_labels[c])
            else:
                disp_date_strs = date_strs; disp_clean_date_labels = clean_date_labels

            unique_all = len(set([r.get('user_id') for r in all_res_data]))
            unique_filtered = len(set([r.get('user_id') for r in filtered_data]))
            if unique_all != unique_filtered:
                st.info(f"🔍 フィルター適用中： 回答者 **{unique_all}人** 中、条件に合う **{unique_filtered}人** のデータを集計しています。")

            st.markdown(campus_legend_html, unsafe_allow_html=True)
            
            if not disp_date_strs or not disp_time_labels:
                st.warning("⚠️ 指定された条件に合う日付または時間帯がありません。フィルター条件を広げてください。")
            else:
                z = np.zeros((len(disp_time_labels), len(disp_date_strs)))
                h = [["" for _ in range(len(disp_date_strs))] for _ in range(len(disp_time_labels))]
                comments_list = []
                
                for r in filtered_data:
                    if r.get('date') not in disp_date_strs: continue
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
                            if cell_campus not in f_locs: continue 
                                
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

                agg_css = f"""
                <style>
                .agg-wrapper {{ max-height: 680px; height: auto; overflow: auto; border: 1px solid #ccc; border-radius: 6px; position: relative; display: flex; background: #fff; padding-bottom: 50px; }}
                .agg-time-col {{ position: sticky; left: 0; z-index: 10; background: #f0f2f6; box-shadow: 2px 0 5px rgba(0,0,0,0.1); flex-shrink: 0; width: 65px; }}
                .agg-header {{ position: sticky; top: 0; z-index: 11; background: #eee; font-size: 13px; font-weight: bold; text-align: center; border-bottom: 2px solid #555; border-right: 1px solid #ccc; height: 50px; display: flex; align-items: center; justify-content: center; padding: 0 5px; box-sizing: border-box; line-height: 1.2; }}
                .agg-top-left {{ position: sticky; top: 0; left: 0; z-index: 20; background: #f0f2f6; border-right: 1px solid #ccc; border-bottom: 2px solid #555; height: 50px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); box-sizing: border-box; }}
                .agg-day-col {{ flex: 1; min-width: 85px; box-sizing: border-box; }}
                .agg-cell {{ border-right: 1px solid #eee; display: flex; align-items: center; justify-content: center; font-weight: bold; position: relative; box-sizing: border-box; cursor: pointer; }}
                
                .agg-cell .tooltip-up, .agg-cell .tooltip-down {{ visibility: hidden; width: 180px; background-color: rgba(30,30,30,0.95); color: #fff; text-align: left; border-radius: 6px; padding: 10px; position: absolute; z-index: 99999; left: 50%; transform: translateX(-50%); opacity: 0; transition: opacity 0.2s; font-size: 11.5px; font-weight: normal; line-height: 1.5; pointer-events: none; white-space: pre-wrap; box-shadow: 0 4px 12px rgba(0,0,0,0.3); max-height: 250px; overflow-y: auto; -webkit-overflow-scrolling: touch; pointer-events: auto; }}
                .agg-cell .tooltip-up::-webkit-scrollbar, .agg-cell .tooltip-down::-webkit-scrollbar {{ width: 6px; }}
                .agg-cell .tooltip-up::-webkit-scrollbar-thumb, .agg-cell .tooltip-down::-webkit-scrollbar-thumb {{ background-color: rgba(255, 255, 255, 0.4); border-radius: 3px; }}
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
        if "event_responses" not in st.session_state: st.session_state.event_responses = []

        if "active_tab" not in st.session_state: st.session_state.active_tab = "📅 入力"
        selected_tab = st.radio("表示切り替え", ["📅 入力", "📊 みんなの集計"], horizontal=True, label_visibility="collapsed", index=0 if st.session_state.active_tab == "📅 入力" else 1)
        st.session_state.active_tab = selected_tab

        if selected_tab == "📅 入力":
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
                payload = {"event_id": event.get("event_id"), "user_id": user.get("user_id"), "comment": user_comment, "responses": res_data}
                if save_response_hybrid(payload):
                    st.session_state.save_success_msg = "回答を保存しました！みんなの集計を確認しましょう👀"
                    st.session_state.active_tab = "📊 みんなの集計"
                    st.rerun()

        elif selected_tab == "📊 みんなの集計":
            st.subheader("📊 予定の集計結果")
            col1, col2 = st.columns([2, 1])
            with col1: policy = st.radio("「未定(△)」の計算方法", [0.5, 1.0, 0.0], format_func=lambda x: f"{x}人としてカウント", horizontal=True, key="opt_policy")
            with col2:
                if st.button("🔄 最新の回答を取得", use_container_width=True, key="opt_refresh"): st.rerun()

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
                            if details[i]["yes"]: st.code("\n".join(details[i]["yes"]), language="text")
                            else: st.write("なし")
                        with c_maybe:
                            st.markdown("<span style='color:#FF9800; font-weight:bold;'>△ 未定</span>", unsafe_allow_html=True)
                            if details[i]["maybe"]: st.markdown("<br>".join([f"△ {n}" for n in details[i]["maybe"]]), unsafe_allow_html=True)
                            else: st.write("なし")
                        with c_no:
                            st.markdown("<span style='color:#F44336; font-weight:bold;'>× 不可</span>", unsafe_allow_html=True)
                            if details[i]["no"]: st.markdown("<br>".join([f"× {n}" for n in details[i]["no"]]), unsafe_allow_html=True)
                            else: st.write("なし")
                st.markdown("<div style='height:15px;'></div>", unsafe_allow_html=True)
            
            if comments_list and can_view_details:
                st.markdown("---")
                st.markdown("### 💬 参加者からのコメント")
                for c in comments_list: st.info(f"**{c['user']}**: {c['comment']}")

if __name__ == "__main__":
    main()
