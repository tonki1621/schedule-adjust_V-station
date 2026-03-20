# 📅 [V-Sync by もっきゅー]

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-FF4B4B.svg)](https://streamlit.io/)
[![Firebase](https://img.shields.io/badge/Database-Firestore-FFCA28.svg?logo=firebase)](https://firebase.google.com/)
[![Google Apps Script](https://img.shields.io/badge/Backend-GAS-F4B400.svg)](https://developers.google.com/apps-script)

**V-station（ボランティアセンター）** の活動を加速させるために開発された、次世代の日程調整・スケジュール管理Webアプリケーションです。

大人数のスタッフ、キャンパスをまたいだ学生たちの複雑なスケジュールを、Firestoreによる**秒速ロード**と、独自開発の**「なぞり塗りエディタ」**によって、ストレスフリーに一元管理します。

🌐 **アプリへのアクセス:** [https://schedule-adjust-v-station.streamlit.app/]

---

## 🎯 背景と目的 (Background & Purpose)

ボランティアセンターでは多数のプロジェクトが同時進行しており、参加学生のキャンパス（なかもず・もりのみや等）や時間割も多岐にわたります。
従来ツールでは「回答の集計に時間がかかる」「スプレッドシートの通信待ちが長い」といった課題がありました。

本アプリは、**「V-stationの運用に100%最適化された、超高速・持続可能な日程調整」**を実現するため、最新のクラウド技術を組み合わせて開発されました。

---

## ✨ 主要機能 (Key Features)

### 1. 直感的な「なぞり塗り」UI
カスタムJavaScriptで構築された独自のエディタを搭載。マウスドラッグやスマホのスワイプ操作で、「可(◯)」「未定(△)」「不可(×)」をペンで色を塗るように連続入力。長押しによる**「所在地（キャンパス）詳細設定」**も可能です。

### 2. 所在地属性とデザインの融合
各回答セルにはキャンパス情報が付与されており、キャンパスごとに異なる模様（格子、斜線、ドット等）をCSSで適用。集計グラフ上でも「誰がどのキャンパスにいるか」が視覚的にひと目で判別できる独自のUXを提供します。

### 3. 3つの柔軟な調整モード
- **🕒 時間帯モード:** 15分刻みの精密な調整。ツールチップが隠れない最適化表示に対応。
- **🏫 時間割モード:** 大学の「1限〜5限・放課後」枠を使用し、空きコマを一括集計。
- **📅 候補リストモード:** 特定の日時やイベント案の多数決を取るアンケート形式。

### 4. ハイブリッドDBによる爆速体験
**Google Sheets ＋ Firestore** のハイブリッド構成を採用。
- **Firestore:** メインDBとして使用。GAS特有の起動待ちを排除し、表示速度を劇的に向上。
- **Google Sheets:** バックアップとして非同期書き出し。次世代の運営者がExcel感覚で中身を確認・修正できる「運用の持続可能性」を確保。

### 5. Discord完全連携＆iCal自動反映
- **Discord通知:** イベント作成時、対象グループ（`@なかもず`等）へ即座にメンション通知。
- **カレンダー連携:** 外部カレンダー（iCal）から自分の予定を自動で「授業等(グレー)」として反映。

---

## 🏗 システム構成 (Architecture)

高いUXとメンテナンス性を両立させるためのサーバーレス構成です。

- **Frontend:** Streamlit (Python), Custom HTML/JS/CSS Components
- **Primary DB:** Firebase Firestore (NoSQLによる高速データ配信)
- **Backup DB / API:** Google Apps Script (GAS) ＋ Google Spreadsheet
- **Hosting:** Streamlit Community Cloud
- **Integration:** Discord Webhook API, iCal Parsing

---

## 🚀 使い方 (How to Use)

### 【一般ユーザー向け】
1. **アカウント作成:** 初回アクセス時に属性（キャンパス・年度）を登録。
2. **時間割の登録:** 「⏰ 時間割設定」から固定スケジュールを登録し、調整時の「一発反映」を活用。
3. **回答する:** 届いたリンクからイベントを開き、予定を塗って提出（変更はいつでも即時反映）。

### 【管理者向け】
1. **イベントの作成:** 「➕ イベント新規作成」から、対象者や時間枠（例：〜18:00）を柔軟に設定して発行。
2. **集計の確認:** 「📊 集計タブ」のヒートマップで参加可能人数を確認。属性フィルタによる詳細分析も可能。
3. **未回答者の確認:** 「⚙️ 管理者専用」から未回答者を1秒で抽出し、リマインドを効率化。

---

## 👨‍💻 開発者 (Author)

**もっきゅー**
- V-stationの持続可能な組織運営と、個々のモチベーション底上げを技術で支援します。
- バグ報告や機能追加の要望は、Discordでお気軽にお寄せください！

---
*© 2026 もっきゅー. All rights reserved.*
