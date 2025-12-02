# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()# ----------------------------------------------------------------------
# 檔名: gui_futures.py
# 作用: 期貨/選擇權/綱要等分頁的交互邏輯與結構定義
# 版本: v1.0.5 (所有主分頁底部新增「資料庫檢視」子分頁)
# 模型名稱: Kimi
# 相關檔案:
# - gui_main.py: 依賴本檔案定義的 Tab 類別
# - data_io.py: 檔案 I/O
# - data_logic.py: 數據匯入與歸檔邏輯
# ----------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Type
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLineEdit,
    QGridLayout, QComboBox, QMessageBox, QLabel, QGroupBox, QSpinBox,
    QSplitter, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QSize

from safety_log import Logger, CustomMessageBox
from data_io import DataIOHandler
from data_logic import DataLogicHandler
from contract_engine import ContractEngine
from config_db import (
    TS_1min_Futures, TS_1min_TW_Stocks, TS_1min_OptionQuotes,
    TS_Continuous_Futures, TS_5min_Archive, TS_Daily_Aggregated,
    ContinuousSpec, ALL_TABLES, engine, Session
)
from sqlalchemy import inspect

# 初始化核心邏輯模組
io_handler = DataIOHandler()
data_logic_handler = DataLogicHandler()
contract_engine = ContractEngine()

# ======================================================================
# 段落作用: 資料庫檢視共用元件 (DBViewWidget)
# ======================================================================

class DBViewWidget(QWidget):
    """可搜尋、排序、多選、全選、刪除的資料庫表格檢視元件。"""
    def __init__(self, table_class: Type):
        super().__init__()
        self.table_class = table_class
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 上排控制列
        ctrl_layout = QHBoxLayout()
        self.table_combo = QComboBox()
        self._load_tables()
        self.table_combo.currentTextChanged.connect(self._load_data)
        ctrl_layout.addWidget(QLabel("表格:"))
        ctrl_layout.addWidget(self.table_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜尋關鍵字...")
        self.search_edit.textChanged.connect(self._filter_data)
        ctrl_layout.addWidget(QLabel("搜尋:"))
        ctrl_layout.addWidget(self.search_edit)

        self.select_all_btn = QPushButton("全選")
        self.select_all_btn.clicked.connect(self._select_all)
        ctrl_layout.addWidget(self.select_all_btn)

        self.del_btn = QPushButton("刪除選取")
        self.del_btn.clicked.connect(self._delete_selected)
        ctrl_layout.addWidget(self.del_btn)

        layout.addLayout(ctrl_layout)

        # 表格
        self.table = QTableWidget()
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # 初始載入
        self._load_data()

    # ------------------------------------------------------------------
    def _load_tables(self):
        """下拉載入所有可用表格名稱。"""
        self.table_combo.clear()
        for tbl in ALL_TABLES:
            self.table_combo.addItem(tbl.__tablename__)

    def _current_table_class(self):
        """依下拉選擇回傳對應 ORM 類別。"""
        name = self.table_combo.currentText()
        for tbl in ALL_TABLES:
            if tbl.__tablename__ == name:
                return tbl
        return self.table_class

    def _load_data(self):
        """載入資料並填入表格。"""
        cls = self._current_table_class()
        try:
            with Session() as s:
                rows = s.query(cls).all()
                if not rows:
                    self.table.setRowCount(0)
                    self.table.setColumnCount(0)
                    return

                # 反射欄位
                insp = inspect(cls)
                cols = [c.key for c in insp.mapper.column_attrs]
                self.table.setRowCount(len(rows))
                self.table.setColumnCount(len(cols))
                self.table.setHorizontalHeaderLabels(cols)

                for r, obj in enumerate(rows):
                    for c, col in enumerate(cols):
                        val = getattr(obj, col)
                        if isinstance(val, datetime):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        self.table.setItem(r, c, QTableWidgetItem(str(val) if val is not None else ''))

        except Exception as e:
            Logger.error(f"載入表格資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "載入失敗", f"載入表格資料時發生錯誤: {e}").exec()

    def _filter_data(self):
        """關鍵字搜尋 (簡單逐欄包含)。"""
        kw = self.search_edit.text().lower()
        for r in range(self.table.rowCount()):
            match = any(
                kw in (self.table.item(r, c).text() or '').lower()
                for c in range(self.table.columnCount())
            )
            self.table.setRowHidden(r, not match)

    def _select_all(self):
        """全選/取消全選切換。"""
        need_select = any(
            not self.table.item(r, 0).isSelected()
            for r in range(self.table.rowCount())
            if not self.table.isRowHidden(r)
        )
        for r in range(self.table.rowCount()):
            if not self.table.isRowHidden(r):
                self.table.selectRow(r) if need_select else self.table.clearSelection()

    def _delete_selected(self):
        """刪除選取列（PK 比對）。"""
        rows = sorted({item.row() for item in self.table.selectedItems()})
        if not rows:
            CustomMessageBox(QMessageBox.Icon.Warning, "提示", "請先選取要刪除的列。").exec()
            return

        reply = CustomMessageBox(
            QMessageBox.Icon.Question, "確認刪除",
            f"確定刪除選取的 {len(rows)} 筆資料？\n(刪除後無法復原)",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
        ).exec()
        if reply != QMessageBox.StandardButton.Ok:
            return

        cls = self._current_table_class()
        insp = inspect(cls)
        pk_cols = [c.key for c in insp.primary_key]

        try:
            with Session() as s:
                for r in rows:
                    # 組建 PK 篩選條件
                    filters = {
                        col: self.table.item(r, c).text()
                        for c, col in enumerate([c.key for c in insp.mapper.column_attrs])
                        if col in pk_cols
                    }
                    obj = s.query(cls).filter_by(**filters).one_or_none()
                    if obj:
                        s.delete(obj)
                s.commit()
            Logger.info(f"已刪除 {len(rows)} 筆資料。")
            self._load_data()  # 重新載入
        except Exception as e:
            Logger.error(f"刪除資料失敗: {e}")
            CustomMessageBox(QMessageBox.Icon.Critical, "刪除失敗", f"刪除資料時發生錯誤: {e}").exec()

# ======================================================================
# 段落作用: 基礎分頁結構 (BasicTabContent)
# ======================================================================

class BasicTabContent(QWidget):
    """
    所有數據匯入和基本操作分頁的通用模板。
    """
    def __init__(self, target_table_class: type, title: str):
        super().__init__()
        self.target_table_class = target_table_class
        self.title = title
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. 數據匯入區塊
        import_group = QGroupBox("數據匯入 (目標表: " + self.target_table_class.__tablename__ + ")")
        import_layout = QGridLayout(import_group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("請選擇 CSV/Excel 檔案路徑...")

        select_file_btn = QPushButton("選擇檔案")
        select_file_btn.clicked.connect(self._select_file)

        import_btn = QPushButton("執行匯入 (UPSERT)")
        import_btn.clicked.connect(self._import_data)

        import_layout.addWidget(QLabel("檔案路徑:"), 0, 0)
        import_layout.addWidget(self.file_path_edit, 0, 1)
        import_layout.addWidget(select_file_btn, 0, 2)
        import_layout.addWidget(import_btn, 1, 1, 1, 2)

        main_layout.addWidget(import_group)

        # 2. 數據操作區塊 (僅預留)
        op_group = QGroupBox("數據操作 (預留)")
        op_layout = QHBoxLayout(op_group)
        op_layout.addWidget(QLabel(f"這是 {self.title} 的操作區塊。"))
        main_layout.addWidget(op_group)

        main_layout.addStretch()

    def _select_file(self):
        """開啟檔案對話框選擇檔案。"""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "選擇要匯入的數據檔案", "", "數據檔案 (*.csv *.xlsx)"
        )
        if file_name:
            self.file_path_edit.setText(file_name)

    def _import_data(self):
        """呼叫 DataLogicHandler 執行數據匯入。"""
        file_path = self.file_path_edit.text()
        if not os.path.exists(file_path):
            CustomMessageBox(QMessageBox.Icon.Warning, "警告", "檔案路徑無效或未選擇檔案。").exec()
            return

        Logger.info(f"嘗試匯入檔案 {file_path} 到 {self.target_table_class.__tablename__}...")

        if data_logic_handler.import_data(file_path, self.target_table_class):
            CustomMessageBox(QMessageBox.Icon.Information, "成功", f"數據成功匯入 {self.target_table_class.__tablename__}！").exec()
        else:
            # 錯誤已在 data_logic_handler 內部處理
            pass


# ======================================================================
# 段落作用: 期貨分頁 (FuturesTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class FuturesTabWidget(QTabWidget):
    """期貨主分頁，包含原始數據、歸檔、連續合約/日K聚合子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.North)

        # 1. 1分鐘原始數據 (TS_1min_Futures)
        self.min1_tab = BasicTabContent(target_table_class=TS_1min_Futures, title="1分鐘原始數據")
        self.addTab(self.min1_tab, "1分鐘 K 線 (TXF/MTX)")

        # 2. 5分鐘歸檔數據 (TS_5min_Archive)
        self.min5_archive_tab = BasicTabContent(target_table_class=TS_5min_Archive, title="5分鐘歸檔數據")
        self.addTab(self.min5_archive_tab, "5分鐘歸檔")

        # 3. 連續合約生成/日K聚合
        self.continuous_tab = self._create_continuous_contract_tab()
        self.addTab(self.continuous_tab, "連續合約 / 日K聚合")

        # 4. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_Futures)
        self.addTab(self.db_view_tab, "資料庫檢視")

    # -------- 以下方法與 v1.0.4 相同，僅行號可能變動 --------
    def _create_continuous_contract_tab(self):
        """創建連續合約和日K聚合操作的分頁 UI。"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        daily_agg_group = QGroupBox("1. 日 K 聚合 (08:45 ~ 05:00 跨日定義)")
        daily_agg_layout = QHBoxLayout(daily_agg_group)
        agg_btn = QPushButton("執行期權日K聚合 (寫入 TS_Daily_Aggregated)")
        agg_btn.clicked.connect(self._run_daily_aggregation)
        daily_agg_layout.addWidget(agg_btn)
        layout.addWidget(daily_agg_group)

        cont_group = QGroupBox("2. 連續合約生成 (使用 TS_Daily_Aggregated)")
        cont_layout = QGridLayout(cont_group)
        self.underlying_combo = QComboBox()
        self.underlying_combo.addItem("TXF")
        self.underlying_combo.addItem("MTX")
        self.roll_rule_combo = QComboBox()
        self.roll_rule_combo.addItem("OI_Crossover")
        self.roll_rule_combo.addItem("FixedDate")
        generate_cont_btn = QPushButton("執行生成連續合約 (寫入 TS_Continuous_Futures)")
        generate_cont_btn.clicked.connect(self._run_continuous_generation)

        cont_layout.addWidget(QLabel("標的代碼:"), 0, 0)
        cont_layout.addWidget(self.underlying_combo, 0, 1)
        cont_layout.addWidget(QLabel("換月規則:"), 1, 0)
        cont_layout.addWidget(self.roll_rule_combo, 1, 1)
        cont_layout.addWidget(generate_cont_btn, 2, 0, 1, 2)
        layout.addWidget(cont_group)
        layout.addStretch()
        return tab

    def _run_daily_aggregation(self):
        """執行 DataLogicHandler 中的日K聚合邏輯。"""
        Logger.info("觸發日K聚合...")
        if data_logic_handler.aggregate_futures_daily_k():
            CustomMessageBox(QMessageBox.Icon.Information, "日K聚合成功", "所有期權 1 分鐘 K 線已成功聚合成日 K 數據。").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "日K聚合失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()

    def _run_continuous_generation(self):
        """執行 ContractEngine 中的連續合約生成邏輯。"""
        underlying_id = self.underlying_combo.currentText()
        roll_rule = self.roll_rule_combo.currentText()
        spec = ContinuousSpec(UnderlyingID=underlying_id, RollRuleType=roll_rule)
        Logger.info(f"觸發連續合約生成: {underlying_id}, 規則: {roll_rule}...")
        if contract_engine.generate_continuous_contract(spec):
            CustomMessageBox(QMessageBox.Icon.Information, "連續合約成功", f"連續合約 {underlying_id} 已成功生成並寫入資料庫！").exec()
        else:
            CustomMessageBox(QMessageBox.Icon.Warning, "連續合約失敗", "請檢查 Log 獲取詳細錯誤訊息。").exec()


# ======================================================================
# 段落作用: 選擇權分頁 (OptionsTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class OptionsTabWidget(QTabWidget):
    """選擇權主分頁，包含 TXO 和其他選擇權的數據匯入子分頁。"""
    def __init__(self):
        super().__init__()
        self.setTabPosition(QTabWidget.TabPosition.South)

        self.txo_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="台指期選擇權 (TXO) 原始數據")
        self.addTab(self.txo_tab, "台指期選擇權 (TXO)")

        self.other_options_tab = BasicTabContent(target_table_class=TS_1min_OptionQuotes, title="其他選擇權原始數據")
        self.addTab(self.other_options_tab, "其他選擇權 (預留)")

        # 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_OptionQuotes)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: 個股/美股分頁 (StocksTabWidget)  –  下方新增資料庫檢視
# ======================================================================

class StocksTabWidget(QTabWidget):
    """個股/美股分頁 (底部加資料庫檢視)。"""
    def __init__(self, title: str):
        super().__init__()
        # 1. 原始匯入頁
        self.import_tab = BasicTabContent(target_table_class=TS_1min_TW_Stocks, title=title)
        self.addTab(self.import_tab, f"{title} 匯入")
        # 2. 資料庫檢視
        self.db_view_tab = DBViewWidget(TS_1min_TW_Stocks)
        self.addTab(self.db_view_tab, "資料庫檢視")


# ======================================================================
# 段落作用: Schema 輸出分頁 (SchemaTabWidget)  –  維持原左右分割
# ======================================================================

class SchemaTabWidget(QWidget):
    """重點綱要與版本更新紀錄 (可編輯儲存) 分頁。"""
    def __init__(self):
        super().__init__()
        main_layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左側：版本更新紀錄（可編輯）
        self.version_text = QTextEdit()
        self._load_version_log()
        splitter.addWidget(self.version_text)

        # 右側：重點綱要 + Schema 輸出
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.schema_label = QLabel("資料庫設計重點綱要：\n1. K線數據按頻率/類型 (1min, 5min, Daily) 分表儲存。\n2. 使用複合主鍵 (ID + 時間) 實現數據去重和高效查詢。\n3. 系統核心邏輯模組化 (Config/Log/IO/DataLogic/Contract/Backtest)。")
        self.schema_label.setWordWrap(True)
        right_layout.addWidget(self.schema_label)

        self.export_json_btn = QPushButton("輸出 Schema 結構 JSON 檔案")
        self.export_json_btn.clicked.connect(self._export_schema)
        right_layout.addWidget(self.export_json_btn)

        self.result_label = QLabel("\nSchema 結構 JSON 輸出結果 (檔案位於 Data 目錄)")
        right_layout.addWidget(self.result_label)

        save_log_btn = QPushButton("儲存版本更新紀錄")
        save_log_btn.clicked.connect(self._save_version_log)
        right_layout.addWidget(save_log_btn)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 1000])
        main_layout.addWidget(splitter)

    # ------------------------------------------------------------------
    def _load_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        if os.path.isfile(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    self.version_text.setPlainText(f.read())
            except Exception as e:
                self.version_text.setPlainText(f'讀取版本更新紀錄失敗：{e}')
                Logger.error(f'讀取版本更新紀錄失敗：{e}')
        else:
            self.version_text.setPlainText('同目錄無「版本更新紀錄.txt」')
            Logger.info('同目錄無「版本更新紀錄.txt」')

    def _save_version_log(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '版本更新紀錄.txt')
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(self.version_text.toPlainText())
            CustomMessageBox(QMessageBox.Icon.Information, "儲存成功", "版本更新紀錄已儲存！").exec()
            Logger.info("版本更新紀錄已儲存。")
        except Exception as e:
            CustomMessageBox(QMessageBox.Icon.Critical, "儲存失敗", f"寫入版本更新紀錄時發生錯誤：{e}").exec()
            Logger.error(f"儲存版本更新紀錄失敗：{e}")

    def _export_schema(self):
        """執行 DataIOHandler 中的 Schema JSON 輸出。"""
        json_path = io_handler.export_database_schema()
        if json_path:
            self.result_label.setText(f"Schema JSON 結構成功輸出至:\n{json_path}")
            CustomMessageBox(QMessageBox.Icon.Information, "成功", "Schema 結構已成功輸出。").exec()
        else:
            self.result_label.setText("Schema JSON 輸出失敗，請檢查 log。")
            CustomMessageBox(QMessageBox.Icon.Critical, "失敗", "Schema 結構輸出失敗。").exec()
