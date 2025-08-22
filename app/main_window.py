# -*- coding: utf-8 -*-
"""
Created on Tue May 20 20:50:00 2025

@author: hkumar
"""

import logging
from functools import partial
from PyQt5.QtWidgets import QMainWindow, QAction, QWidget, QStackedLayout

# Module widgets
from di_testing.di_testing_widget import DITestingWidget
from db_comparison.comparison_module import ComparisonTabMainWidget
from sftp_connection.sftp_tab import SFTPConnectionTab
from dcs.dcs_plotting_widget import SQLiteDBsComparisonPlottingWidget

from utils.clean_process import kill_child_processes
# Log viewer
from logging_.log_viewer_dialog import LogViewerDialog
from logging_.log_widget import LogWidget

logger = logging.getLogger(__name__)

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("ClariFI DI")
        self.setGeometry(100, 100, 1600, 900)

        self._log_dialog_instance = None
        self._log_widget_for_dialog = LogWidget(self)
        self._current_widget = None

        self._setup_modules()
        self._setup_menu()

        # Load DI Testing by default
        self._activate_module("DI Testing")
        logger.info("MainWindow initialized.")

    def _setup_modules(self):
        self.central_container = QWidget(self)
        self.stacked_layout = QStackedLayout()
        self.central_container.setLayout(self.stacked_layout)
        self.setCentralWidget(self.central_container)

        self.module_configs = {
            "DI Testing": {
                "instance": None,
                "factory": lambda: DITestingWidget(self),
            },
            "SFTP Connection": {
                "instance": None,
                "factory": lambda: SFTPConnectionTab(parent_logger=logging.getLogger(), parent=self),
            },
            "DB Comparison": {
                "instance": None,
                "factory": lambda: ComparisonTabMainWidget(parent_logger=logging.getLogger(), parent=self),
            },

            "DCS Stats Charts":{
                "instance": None,
                "factory": lambda: SQLiteDBsComparisonPlottingWidget(parent=self),
                }
        }

        # Preload DI Testing
        self._activate_module("DI Testing")

    def _setup_menu(self):
        menu_bar = self.menuBar()

        # File Menu
        file_menu = menu_bar.addMenu("&File")
        exit_action = QAction("&Exit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Modules Menu
        modules_menu = menu_bar.addMenu("&Modules")
        for i, module_name in enumerate(self.module_configs.keys(), start=1):
            action = QAction(f"&{module_name}", self)
            action.setShortcut(f"Ctrl+{i}")
            action.triggered.connect(partial(self._activate_module, module_name))
            modules_menu.addAction(action)

        # View Menu
        view_menu = menu_bar.addMenu("&View")
        log_action = QAction("&Application Log", self)
        log_action.setShortcut("Ctrl+L")
        log_action.triggered.connect(self.show_log_viewer)
        view_menu.addAction(log_action)

        logger.debug("Menu setup complete.")

    def _activate_module(self, module_name):
        config = self.module_configs[module_name]

        if config["instance"] is None:
            config["instance"] = config["factory"]()
            self.stacked_layout.addWidget(config["instance"])
            logger.debug(f"{module_name} module widget created and added to stack.")

        self.stacked_layout.setCurrentWidget(config["instance"])
        logger.info(f"{module_name} module activated.")

    def get_log_text_edit_for_setup(self):
        return self._log_widget_for_dialog.log_display

    def show_log_viewer(self):
        if self._log_dialog_instance is None:
            self._log_dialog_instance = LogViewerDialog(self._log_widget_for_dialog, self)
            logger.debug("LogViewerDialog created.")

        if self._log_dialog_instance.isHidden():
            self._log_dialog_instance.show()
        else:
            self._log_dialog_instance.activateWindow()
            self._log_dialog_instance.raise_()
        logger.info("Log viewer requested.")

    def closeEvent(self, event):
        logger.info("Application closing.")
        if self._log_dialog_instance and self._log_dialog_instance.isVisible():
            self._log_dialog_instance.close()

        # Cleanup logic for individual modules if needed
        db = self.module_configs["DB Comparison"]["instance"]
        if db and hasattr(db, 'cleanup_on_close'):
            db.cleanup_on_close()

        di = self.module_configs["DI Testing"]["instance"]
        if di:
            left_pane = getattr(di, 'left_pane', None)
            if left_pane and hasattr(left_pane, 'worker') and left_pane.worker is not None:
                logger.info("Attempting to cancel DI testing worker on close...")
                left_pane.on_cancel()

        sftp = self.module_configs["SFTP Connection"]["instance"]
        if sftp and hasattr(sftp, "closeEvent"):
            sftp.closeEvent()

        dcs_charts = sftp.module_configs['DCS Stats Charts']["instance"]
        if dcs_charts:
            if hasattr(dcs_charts, 'closeEvent'):
                dcs_charts.closeEvent()

        super().closeEvent(event)
