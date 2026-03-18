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

st.set_page_config(page_title="V-Sync by もっきゅー", layout="wide")

# 💡 ご自身のStreamlitアプリのURLに変更してください
APP_BASE_URL = "https://schedule-adjust-v-station.streamlit.app/"

# ==========================================
# UX改善: ロード中表示＆時間割テーブル用CSS
# ==========================================
st.markdown("""
    <style>
        .stDeployStatus, [data-testid="stStatusWidget"] label { display: none !important; }
        [data-testid="stStatusWidget"] { visibility: visible !important; display: flex !important; position: fixed !important; top: 50% !important; left: 50% !important; transform: translate(-50%, -50%) !important; background: rgba(255, 255, 255, 0.95) !important; color: #333 !important; padding: 20px 40px !important; border-radius: 12px !important; z-index: 999999 !important; box-shadow: 0 8px 24px rgba(0,0,0,0.15) !important; border: 2px solid #4CAF50 !important; text-align: center !important; justify-content: center !important; }
        [data-testid="stStatusWidget"]::after { content: "⏳ 通信中 \\A 処理しています..."; white-space: pre-wrap; font-size: 20px !important; font-weight: bold !important; line-height: 1.5 !important; }
        @media (max-width: 600px) { [data-testid="stStatusWidget"] { padding: 15px 20px !important; width: 80% !important; } [data-testid="stStatusWidget"]::after { font-size: 16px !important; } }
        .stApp, .stApp [data-testid="stAppViewBlockContainer"], div[data-testid="stVerticalBlock"], div[data-testid="stForm"], iframe { opacity: 1 !important; transition: none !important; filter: none !important; }
        .user-header { display: flex; align-items: center; justify-content: space-between; background: #f8f9fa; padding: 10px 20px; border-radius: 8px; border-left: 5px solid #4CAF50; margin-bottom: 20px; }
        .event-desc { background: #fff8e1; padding: 15px; border-radius: 8px; border-left: 4px solid #ffc107; margin-bottom: 20px; font-size: 14px; line-height: 1.6; }
        .event-desc a { color: #2196F3; font-weight: bold; text-decoration: none; }
        .event-desc a:hover { text-decoration: underline; }
        .tt-day-header { font-size: 16px; font-weight: bold; background: #4CAF50; color: white; padding: 8px; border-radius: 6px; text-align: center; }
        .tt-time-cell { font-size: 14px; font-weight: bold; background: #f0f2f6; padding: 10px; border-radius: 6px; text-align: center; border-left: 4px solid #4CAF50;}
        .tt-time-sub { font-size: 11px; color: #666; font-weight: normal; }
        .status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #4CAF50, #45a049); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(76,175,80,0.3); letter-spacing: 0.5px;}
        .af-status-on { color: #fff; font-weight: bold; background: linear-gradient(135deg, #2196F3, #1976D2); padding: 4px 0; border-radius: 6px; border: none; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block; box-shadow: 0 2px 4px rgba(33,150,243,0.3); letter-spacing: 0.5px;}
        .status-off { color: #9e9e9e; background: #ffffff; padding: 4px 0; border-radius: 6px; border: 1px dashed #d0d0d0; font-size: 12px; text-align: center; margin-top: -10px; margin-bottom: 5px; display: block;}
        
        .tt-wrapper { overflow-x: auto; background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 0; box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-bottom: 20px;}
        .tt-table { width: 100%; min-width: 280px; border-collapse: collapse; table-layout: fixed; }
        .tt-table th, .tt-table td { padding: 5px 0px; text-align: center; border-bottom: 1px solid #eee; }
        .tt-table th { font-weight: bold; background: #f8f9fa; color: #333; position: sticky; top: 0; font-size: 12px; padding: 8px 0px;}
        
        .tt-table td:first-child { font-weight: bold; background: #f0f2f6; border-right: 2px solid #ddd; text-align: center; width: 45px; font-size: 11px; padding: 5px 2px;}
        .tt-table td:first-child span { font-size: 9px; color: #666; display: block; font-weight: normal; letter-spacing: -0.5px; margin-top: -2px;}
        
        .tt-table [data-testid="stCheckbox"] { justify-content: center; margin: 0 !important; padding: 0 !important; width: 100% !important;}
        .tt-table [data-testid="stCheckbox"] label { min-height: 0 !important; padding: 0 !important; gap: 0 !important; }
        .tt-table [data-testid="stCheckbox"] div[role="checkbox"] { margin: 0 auto !important; }
        .tt-table [data-testid="stCheckbox"] p { display: none !important; }
    </style>
""", unsafe_allow_html=True)

GAS_URL = "https://script.google.com/macros/s/AKfycby7hAc1_dhSQ_tJzSiJeSc2Ez7pgaeVTrVL5fOIZPNNZ-_YLke236yGgCgj3yijhQHh/exec"

# ==========================================
# コンポーネント (rt_editor, options_editor, grid_editor)
# ==========================================

os.makedirs("rt_editor", exist_ok=True)
with open("rt_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""
    <!DOCTYPE html><html><head><meta charset="utf-8"><style>
        body { font-family: sans-serif; margin: 0; padding: 0; background: transparent;}
        .editor-container { border: 1px solid #ccc; border-radius: 6px; overflow: hidden; background: #fff; }
        .toolbar { background: #f8f9fb; padding: 6px; border-bottom: 1px solid #ccc; display: flex; gap: 5px; flex-wrap: wrap; align-items: center; }
        .toolbar button { background: #fff; border: 1px solid #ccc; border-radius: 4px; padding: 4px 10px; font-size: 13px; cursor: pointer; color: #333; transition: 0.2s; }
        .toolbar button:hover { background: #e9ecef; }
        textarea { width: 100%; height: 120px; border: none; padding: 10px; font-size: 14px; resize: vertical; outline: none; box-sizing: border-box; font-family: inherit; line-height: 1.5; }
    </style></head><body>
    <div class="editor-container">
        <div class="toolbar">
            <button onclick="insertTag('<b>', '</b>')" title="太字"><b>B</b> 太字</button>
            <button onclick="insertTag('<i>', '</i>')" title="斜体"><i>I</i> 斜体</button>
            <div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div>
            <button onclick="insertRed()" title="赤文字"><span style="color:#FF4B4B; font-weight:bold;">A</span> 赤</button>
            <button onclick="insertBlue()" title="青文字"><span style="color:#2196F3; font-weight:bold;">A</span> 青</button>
            <div style="width: 1px; height: 20px; background: #ccc; margin: 0 4px;"></div>
            <button onclick="insertLink()" title="リンク">🔗 リンク追加</button>
        </div>
        <textarea id="editor" placeholder="📝 イベントの説明や注意事項を入力..."></textarea>
    </div>
    <script>
        function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); }
        function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); }
        function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); }
        const editor = document.getElementById('editor'); let timer;
        function sendValue() { setComponentValue(editor.value); }
        function insertTag(startTag, endTag) {
            const start = editor.selectionStart; const end = editor.selectionEnd; const val = editor.value; const selected = val.substring(start, end);
            editor.value = val.substring(0, start) + startTag + selected + endTag + val.substring(end); editor.focus();
            editor.selectionStart = start + startTag.length; editor.selectionEnd = end + startTag.length; sendValue();
        }
        function insertRed() { insertTag("<span style='color:#FF4B4B; font-weight:bold;'>", "</span>"); }
        function insertBlue() { insertTag("<span style='color:#2196F3; font-weight:bold;'>", "</span>"); }
        function insertLink() {
            const url = prompt('リンク先のURLを入力', 'https://');
            if (url) { const text = prompt('表示するテキストを入力', 'こちらをクリック'); if (text) { const linkTag = `<a href='${url}' target='_blank'>${text}</a>`; const start = editor.selectionStart; const val = editor.value; editor.value = val.substring(0, start) + linkTag + val.substring(editor.selectionEnd); sendValue(); } }
        }
        editor.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(sendValue, 500); });
        editor.addEventListener('blur', sendValue);
        window.addEventListener("message", function(event) { if (event.data.type === "streamlit:render") { sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.body.scrollHeight + 15}); } });
        init();
    </script></body></html>
    """)
rt_editor = components.declare_component("rt_editor", path="rt_editor")


os.makedirs("options_editor", exist_ok=True)
with open("options_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""
    <!DOCTYPE html><html><head><meta charset="utf-8"><style>
    body{margin:0;font-family:sans-serif;}
    .opt-card { background:#fff; border:1px solid #e0e0e0; border-radius:12px; padding:15px; margin-bottom:15px; box-shadow:0 2px 5px rgba(0,0,0,0.05); }
    .opt-title { font-size:18px; font-weight:bold; color:#2e7d32; margin-bottom:15px; text-align:center; }
    .btn-group { display:flex; gap:12px; }
    .opt-btn { flex:1; padding:20px 0; border-radius:12px; border:2px solid #ddd; background:#fff; font-size:18px; font-weight:bold; cursor:pointer; transition:all 0.2s cubic-bezier(0.175, 0.885, 0.32, 1.275); color:#555; text-align:center; }
    .opt-btn[data-v="1"].active { background:#4CAF50; color:#fff; border-color:#4CAF50; box-shadow:0 6px 12px rgba(76,175,80,0.4); transform: translateY(-3px); }
    .opt-btn[data-v="2"].active { background:#FFEB3B; color:#333; border-color:#FBC02D; box-shadow:0 6px 12px rgba(255,235,59,0.4); transform: translateY(-3px); }
    .opt-btn[data-v="0"].active { background:#f5f5f5; color:#777; border-color:#ccc; transform: translateY(-3px); }
    #submit-btn { width: 100%; padding: 18px; background-color: #FF4B4B; color: white; border: none; border-radius: 12px; font-size: 20px; cursor: pointer; font-weight: bold; box-shadow: 0 6px 12px rgba(0,0,0,0.15); margin-top: 10px; transition:0.2s; }
    #submit-btn:hover { background-color: #e63946; transform: translateY(-2px); }
    textarea { width: 100%; padding: 15px; border: 1px solid #ccc; border-radius: 12px; font-family: inherit; font-size: 16px; margin-bottom:10px; resize:vertical; box-sizing: border-box; }
    </style></head><body>
    <div id="content"></div>
    <script>
    function sendMessageToStreamlitClient(type, data) { window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data), "*"); }
    function init() { sendMessageToStreamlitClient("streamlit:componentReady", {apiVersion: 1}); }
    function setComponentValue(value) { sendMessageToStreamlitClient("streamlit:setComponentValue", {value: value, dataType: "json"}); }

    let optsData = [];
    let myComment = "";
    
    window.addEventListener("message", function(event) {
        if (event.data.type === "streamlit:render") {
            const args = event.data.args;
            if(window.lastEventId === args.eventId && window.lastSaveTs === args.saveTs) return; 
            window.lastEventId = args.eventId;
            window.lastSaveTs = args.saveTs;
            
            const opts = args.options;
            const myAnsBin = args.myAnsBin;
            myComment = args.myComment || "";
            const isClosed = args.isClosed;
            
            let html = "";
            optsData = [];
            
            opts.forEach((opt, i) => {
                let v = i < myAnsBin.length ? parseInt(myAnsBin[i]) : 0;
                optsData.push(v);
                let pointerEv = isClosed ? "pointer-events:none; opacity:0.7;" : "";
                
                html += `
                <div class="opt-card" style="${pointerEv}">
                    <div class="opt-title">📅 ${opt}</div>
                    <div class="btn-group" id="group-${i}">
                        <button class="opt-btn ${v===0 ? 'active':''}" data-v="0" onclick="setOpt(${i}, 0)">× 不可</button>
                        <button class="opt-btn ${v===2 ? 'active':''}" data-v="2" onclick="setOpt(${i}, 2)">△ 未定</button>
                        <button class="opt-btn ${v===1 ? 'active':''}" data-v="1" onclick="setOpt(${i}, 1)">◯ 可</button>
                    </div>
                </div>`;
            });
            
            if(!isClosed) {
                html += `
                <div class="opt-card">
                    <div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント</div>
                    <textarea id="comment-box" rows="2" placeholder="遅刻・早退などの連絡事項">${myComment}</textarea>
                    <button id="submit-btn" onclick="submitData()">✅ 回答を保存して提出</button>
                </div>`;
            } else {
                html += `
                <div class="opt-card">
                    <div style="font-size:16px; font-weight:bold; margin-bottom:10px; color:#333;">📝 自分の備考・コメント</div>
                    <div style="padding:15px; background:#eee; border-radius:12px; min-height:50px; font-size:16px;">${myComment}</div>
                </div>`;
            }
            
            document.getElementById("content").innerHTML = html;
            setTimeout(() => sendMessageToStreamlitClient("streamlit:setFrameHeight", {height: document.body.scrollHeight + 50}), 150);
        }
    });
    
    window.setOpt = function(idx, val) {
        optsData[idx] = val;
        const btns = document.getElementById('group-' + idx).querySelectorAll('.opt-btn');
        btns.forEach(b => b.classList.remove('active'));
        document.getElementById('group-' + idx).querySelector(`[data-v="${val}"]`).classList.add('active');
    };
    
    window.submitData = function() {
        const btn = document.getElementById("submit-btn");
        btn.innerText = "⏳ 保存処理中...";
        btn.style.pointerEvents = "none";
        const comment = document.getElementById("comment-box").value;
        setComponentValue({
            trigger_save: true,
            binary: optsData.join(''),
            comment: comment,
            ts: Date.now()
        });
    };
    init();
    </script></body></html>
    """)
options_editor = components.declare_component("options_editor", path="options_editor")


os.makedirs("custom_editor", exist_ok=True)
with open("custom_editor/index.html", "w", encoding="utf-8") as f:
    f.write("""
    <!DOCTYPE html><html><head><meta charset="utf-8"><style>
    body{margin:0;font-family:sans-serif;} *{box-sizing:border-box;}
    .pen-btn { padding: 0; border-radius: 50%; width: 45px; height: 45px; border: none; cursor: pointer; font-weight: bold; font-size: 14px; transition: 0.2s; display: flex; align-items: center; justify-content: center; box-shadow: 0 2px 4px rgba(0,0,0,0.15); margin: 0 auto; text-align: center; line-height: 1.1; }
    .pen-btn.active { border: 3px solid #333 !important; transform: scale(1.1); box-shadow: 0 4px 8px rgba(0,0,0,0.3); }
    
    #detail-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 999999; justify-content: center; align-items: center; backdrop-filter: blur(2px); }
    .modal-content { background: #fff; width: 320px; padding: 20px; border-radius: 12px; box-shadow: 0 8px 24px rgba(0,0,0,0.2); position: relative; }
    .modal-title { font-size: 16px; font-weight: bold; color: #333; margin-bottom: 10px; border-bottom: 2px solid #4CAF50; padding-bottom: 5px; }
    .modal-label { font-size: 12px; font-weight: bold; color: #666; margin-top: 15px; display: block; }
    .modal-select, .modal-input { width: 100%; padding: 8px; margin-top: 5px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
    
    .status-switch { display: flex; gap: 8px; margin-top: 5px; }
    .sw-btn { flex: 1; padding: 8px; border: 1px solid #ddd; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: bold; background: #f9f9f9; color: #555; transition: 0.2s; }
    .sw-btn.active[data-v="1"] { background: #4CAF50; color: white; border-color: #4CAF50; }
    .sw-btn.active[data-v="2"] { background: #FFEB3B; color: #333; border-color: #FBC02D; }
    .sw-btn.active[data-v="0"] { background: #fff; color: #333; border-color: #999; }
    
    .modal-btns { display: flex; gap: 10px; margin-top: 20px; }
    .modal-btn-save { flex: 1; background: #4CAF50; color: white; border: none; padding: 12px; border-radius: 6px; font-weight: bold; cursor: pointer; }
    .modal-btn-save:hover { background: #45a049; }
    
    .memo-icon { position: absolute; top: 1px; right: 2px; font-size: 10px; line-height: 1; filter: drop-shadow(1px 1px 1px rgba(255,255,255,0.8)); pointer-events: none;}
    .c { position: relative; transition: filter 0.1s; }
    
    @keyframes pressAnim {
        0% { transform: scale(1); filter: brightness(1); }
        100% { transform: scale(0.92); filter: brightness(0.8); box-shadow: inset 0 4px 8px rgba(0,0,0,0.3); }
    }
    .pressing { animation: pressAnim 0.4s forwards; z-index: 100; }
    
    #palette-header { background: #eee; border-radius: 8px 8px 0 0; margin: -12px -8px 8px -8px; padding: 8px; font-size: 12px; font-weight: bold; color: #555; text-align: center; cursor: move; user-select: none; }
    </style></head><body>
    
    <div id="palette" style="fixed; top:20px; right:30px; z-index:99999; background:rgba(255,255,255,0.95); border:1px solid #ddd; border-radius:12px; box-shadow:0 8px 24px rgba(0,0,0,0.15); padding:12px 8px; display:none; flex-direction:column; gap:12px; backdrop-filter: blur(8px);">
        <div id="palette-header">🤚 ドロップで移動</div>
        <button class="pen-btn active" onclick="window.setPen(1)" id="pen-1" style="background:#4CAF50; color:#fff; font-size:11px;">可</button>
        <button class="pen-btn" onclick="window.setPen(2)" id="pen-2" style="background:#FFEB3B; color:#333; font-size:11px;">未定</button>
        <button class="pen-btn" onclick="window.setPen(0)" id="pen-0" style="background:#fff; color:#333; border:1px solid #ccc; font-size:11px;">消す</button>
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
                <option value="杉本">杉本</option>
                <option value="阿倍野">阿倍野</option>
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
    window.cellDetails = {}; 
    let defaultCampus = ""; 
    let modalStatus = 1;

    const palette = document.getElementById('palette');
    const pHeader = document.getElementById('palette-header');
    let isDraggingPalette = false; let offsetX, offsetY;

    pHeader.addEventListener('mousedown', e => { isDraggingPalette = true; offsetX = e.clientX - palette.getBoundingClientRect().left; offsetY = e.clientY - palette.getBoundingClientRect().top; });
    window.addEventListener('mousemove', e => { if (!isDraggingPalette) return; palette.style.left = (e.clientX - offsetX) + 'px'; palette.style.top = (e.clientY - offsetY) + 'px'; palette.style.right = 'auto'; });
    window.addEventListener('mouseup', () => { isDraggingPalette = false; });
    pHeader.addEventListener('touchstart', e => { isDraggingPalette = true; const touch = e.touches[0]; offsetX = touch.clientX - palette.getBoundingClientRect().left; offsetY = touch.clientY - palette.getBoundingClientRect().top; }, {passive: false});
    window.addEventListener('touchmove', e => { if (!isDraggingPalette) return; const touch = e.touches[0]; palette.style.left = (touch.clientX - offsetX) + 'px'; palette.style.top = (touch.clientY - offsetY) + 'px'; palette.style.right = 'auto'; e.preventDefault(); }, {passive: false});
    window.addEventListener('touchend', () => { isDraggingPalette = false; });

    const modalBg = document.getElementById('detail-modal');
    modalBg.addEventListener('mousedown', function(e) { if(e.target === this) closeModal(); });
    modalBg.addEventListener('touchstart', function(e) { if(e.target === this) closeModal(); }, {passive: true});

    window.setModalStatus = function(v) {
        modalStatus = v;
        document.querySelectorAll('.sw-btn').forEach(b => {
            b.classList.toggle('active', parseInt(b.dataset.v) === v);
        });
    };

    window.upd = function(el, v) { 
        el.dataset.v = v; 
        const key = `${el.dataset.r}_${el.dataset.c}`;
        
        let detail = window.cellDetails[key];
        
        if (v == 0) {
            delete window.cellDetails[key];
            detail = null;
        } else if ((v == 1 || v == 2) && detail) {
            if (detail.campus === defaultCampus && !detail.note) {
                delete window.cellDetails[key];
                detail = null;
            }
        } else if ((v == 1 || v == 2) && defaultCampus && !detail) {
            window.cellDetails[key] = {campus: defaultCampus, note: ""};
            detail = window.cellDetails[key];
        }

        let campus = detail ? detail.campus : ((v == 1 || v == 2) ? defaultCampus : "");
        let bgImage = 'none';
        let bgColor = '#fff';

        if (v == 1) bgColor = '#4CAF50';
        else if (v == 2) bgColor = '#FFEB3B';
        else if (v == 3) bgColor = '#e0e0e0';

        if (v == 1 || v == 2 || v == 3) {
            let cColor = (v == 3) ? 'rgba(255,255,255,0.7)' : 'rgba(255,255,255,0.3)';
            let cColorDark = (v == 3) ? 'rgba(0,0,0,0.1)' : 'rgba(0,0,0,0.15)';
            if (campus === "杉本") bgImage = `repeating-linear-gradient(45deg, ${cColor}, ${cColor} 4px, transparent 4px, transparent 8px)`;
            else if (campus === "あべの" || campus === "阿倍野") bgImage = `repeating-linear-gradient(-45deg, ${cColorDark}, ${cColorDark} 4px, transparent 4px, transparent 8px)`;
            else if (campus === "りんくう") bgImage = `radial-gradient(circle, ${cColor} 3px, transparent 4px)`;
            else if (campus === "もりのみや") bgImage = `repeating-linear-gradient(90deg, ${cColor}, ${cColor} 4px, transparent 4px, transparent 8px)`;
            else if (campus === "その他/移動中") bgImage = `repeating-linear-gradient(45deg, ${cColor}, ${cColor} 2px, transparent 2px, transparent 4px), repeating-linear-gradient(-45deg, ${cColor}, ${cColor} 2px, transparent 2px, transparent 4px)`;
            else if (v == 3 && !campus) bgImage = `repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(255,255,255,.8) 4px, rgba(255,255,255,.8) 8px)`; 
        }

        el.style.background = bgColor;
        el.style.backgroundImage = bgImage;
        if (campus === "りんくう") el.style.backgroundSize = '10px 10px';
        else el.style.backgroundSize = 'auto';

        const existingIcon = el.querySelector('.memo-icon');
        if (detail && (detail.campus || detail.note)) {
            if (!existingIcon) el.insertAdjacentHTML('beforeend', '<div class="memo-icon">💬</div>');
        } else {
            if (existingIcon) existingIcon.remove();
        }
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
                    const r = (typeof item === 'object') ? item.row : item;
                    const campus = (typeof item === 'object') ? item.campus : "";
                    const cell = document.querySelector(`[data-r="${r}"][data-c="${c}"]`);
                    if(cell) {
                        const cellKey = `${r}_${c}`;
                        if (campus === "💼 バイト/私用") {
                            window.cellDetails[cellKey] = {campus: "", note: "バイト/私用"};
                            window.upd(cell, 0); 
                        } else if (campus) {
                            window.cellDetails[cellKey] = {campus: campus, note: "定期授業"};
                            window.upd(cell, 3);
                        } else {
                            window.upd(cell, 3);
                        }
                    }
                });
            }
        }
        const origText = btnEl.innerHTML; btnEl.innerHTML = "✅ 反映完了！"; setTimeout(() => btnEl.innerHTML = origText, 2000);
    };
    
    window.toggleList = function(id) { const el = document.getElementById(id); el.style.display = el.style.display === 'none' ? 'block' : 'none'; };
    document.addEventListener('click', function(e) { if(!e.target.closest('.ms-container')) { document.querySelectorAll('.ms-options').forEach(el => el.style.display = 'none'); } });

    let selectedMode = 1;
    window.setPen = function(mode) {
        selectedMode = mode;
        [0, 1, 2].forEach(m => {
            const b = document.getElementById('pen-' + m);
            b.classList.remove('active');
        });
        document.getElementById('pen-' + mode).classList.add('active');
    };

    let editingCell = null;
    window.openModal = function(cell) {
        editingCell = cell;
        const r = cell.dataset.r; const c = cell.dataset.c;
        const key = `${r}_${c}`;
        const detail = window.cellDetails[key] || {campus: defaultCampus, note: ""};
        
        setModalStatus(parseInt(cell.dataset.v) || 1);
        
        document.getElementById('modal-campus').value = detail.campus || "";
        document.getElementById('modal-note').value = detail.note || "";
        document.getElementById('detail-modal').style.display = 'flex';
    };

    window.closeModal = function() {
        document.getElementById('detail-modal').style.display = 'none';
        if (editingCell) {
            editingCell.classList.remove('pressing');
            editingCell = null;
        }
    };

    window.saveModal = function() {
        if(!editingCell) return;
        const r = editingCell.dataset.r; const c = editingCell.dataset.c;
        const key = `${r}_${c}`;
        const campus = document.getElementById('modal-campus').value;
        const note = document.getElementById('modal-note').value.trim();

        if(campus || note || modalStatus === 0) {
            window.cellDetails[key] = {campus: campus, note: note};
            window.upd(editingCell, modalStatus);
        } else {
            delete window.cellDetails[key];
            window.upd(editingCell, modalStatus);
        }
        closeModal();
    };

    window.addEventListener("message", function(event) {
        if (event.data.type === "streamlit:render") {
            const args = event.data.args; 
            document.getElementById("content").innerHTML = args.html_code;
            totalDays = args.cols; numRows = args.rows; unavailColRows = args.unavailColRows || {};
            window.cellDetails = args.cellDetails || {};
            defaultCampus = args.defaultCampus || ""; 
            
            document.getElementById('pen-1').innerHTML = defaultCampus ? `可<br><span style='font-size:9px;'>(${defaultCampus})</span>` : "可";
            
            if(window.lastEventId !== args.eventId) { currentWeek = 0; window.lastEventId = args.eventId; }
            window.renderWeek();
            
            if(args.isClosed) { document.getElementById('palette').style.display = 'none'; return; } 
            else { document.getElementById('palette').style.display = 'flex'; }
            
            window.addEventListener('contextmenu', function(e) { e.preventDefault(); e.stopPropagation(); return false; }, { capture: true });

            const g = document.getElementById('g'); if(!g) return;
            
            let down = false; let isErasing = false; let pressTimer = null; let isLongPress = false; let startX = 0, startY = 0;
            
            const handleStart = (e, x, y, shift) => {
                const cell = e.target.closest('.c');
                if(!cell) return; 
                
                down = true; isErasing = shift; isLongPress = false; startX = x; startY = y;
                window.upd(cell, isErasing ? 0 : selectedMode); 
                cell.classList.add('pressing'); 
                
                pressTimer = setTimeout(() => {
                    isLongPress = true; down = false; cell.classList.remove('pressing'); openModal(cell);
                }, 400);
            };

            const handleMove = (e, x, y) => {
                if(!down) return;
                if (Math.abs(x - startX) > 10 || Math.abs(y - startY) > 10) {
                    clearTimeout(pressTimer);
                    document.querySelectorAll('.pressing').forEach(el => el.classList.remove('pressing'));
                }
                if(!isLongPress) {
                    const cell = document.elementFromPoint(x, y)?.closest('.c');
                    if(cell) window.upd(cell, selectedMode); 
                }
            };

            const handleEnd = () => {
                if (down && pressTimer) clearTimeout(pressTimer);
                document.querySelectorAll('.pressing').forEach(el => el.classList.remove('pressing')); 
                down = false;
            };

            g.onmousedown = e => handleStart(e, e.clientX, e.clientY, e.shiftKey);
            g.onmousemove = e => handleMove(e, e.clientX, e.clientY);
            window.onmouseup = handleEnd;
            window.onmouseleave = handleEnd; 

            g.addEventListener('touchstart', e => { 
                handleStart(e, e.touches[0].clientX, e.touches[0].clientY, false);
                if(!isLongPress) e.preventDefault(); 
            }, {passive: false});
            
            g.addEventListener('touchmove', e => { 
                if(down) { handleMove(e, e.touches[0].clientX, e.touches[0].clientY); e.preventDefault(); }
            }, {passive: false});
            
            g.addEventListener('touchend', handleEnd);
            
            const btn = document.getElementById("submit-btn");
            if(btn) { btn.onclick = () => { 
                const res = Array.from({length: numRows}, (_, r) => Array.from({length: totalDays}, (_, c) => parseInt(document.querySelector(`[data-r="${r}"][data-c="${c}"]`).dataset.v))); 
                const commentText = document.getElementById("comment-box").value; 
                setComponentValue({ data: res, comment: commentText, cell_details: window.cellDetails, trigger_save: true, ts: Date.now() }); 
                btn.innerText = "⏳ 保存処理中..."; btn.style.backgroundColor = "#ff7b7b"; btn.style.pointerEvents = "none"; document.getElementById('palette').style.display = 'none'; 
            }; }
            
            document.querySelectorAll('.c').forEach(cell => { window.upd(cell, cell.dataset.v); });
        }
    }); init(); </script></body></html>
    """)
grid_editor = components.declare_component("grid_editor", path="custom_editor")
