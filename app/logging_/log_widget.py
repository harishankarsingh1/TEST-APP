import logging
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QTextEdit, QPushButton, QHBoxLayout, QComboBox, QLabel
from PyQt5.QtGui import QFont
from PyQt5.QtCore import pyqtSignal

class LogWidget(QWidget):
    level_changed = pyqtSignal(str) # Signal to indicate log level filter changed

    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        font = QFont("Consolas", 10) 
        if font.family() != "Consolas": 
            font = QFont("Monospace", 10)
        self.log_display.setFont(font)
        self.log_display.setObjectName("LogDisplay") 

        # Log Level Filter
        self.level_filter_label = QLabel("Filter Level:")
        self.level_filter_combo = QComboBox()
        self.log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        self.level_filter_combo.addItems(self.log_levels)
        self.level_filter_combo.setCurrentText("INFO") # Default filter level
        self.level_filter_combo.currentTextChanged.connect(self.level_changed.emit)


        self.clear_button = QPushButton("Clear Log")
        
        controls_layout = QHBoxLayout()
        controls_layout.addWidget(self.level_filter_label)
        controls_layout.addWidget(self.level_filter_combo)
        controls_layout.addStretch()
        controls_layout.addWidget(self.clear_button)
        # controls_layout.addStretch() # Removed to keep clear button to the right of stretch

        layout = QVBoxLayout(self)
        layout.addLayout(controls_layout) # Add controls at the top
        layout.addWidget(self.log_display, 1) # Log display takes most space
        self.setLayout(layout)