import logging
from PyQt5.QtWidgets import QWidget, QHBoxLayout, QSplitter 
from PyQt5.QtCore import Qt, pyqtSlot
import pandas as pd 

from .left_pane_widget import LeftPaneWidget
from .right_pane_widget import RightPaneWidget


logger = logging.getLogger(__name__)

class DITestingWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DITestingModule")

        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(0,0,0,0) 

        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)

        # Initialize Right Pane first, so it can be passed to Left Pane if needed for direct calls
        self.right_pane = RightPaneWidget(self)
        # self.right_pane.setStyleSheet("border: 1px solid gray; border-radius: 4px; padding: 2px;")
        
        # Initialize Left Pane (Configurations), passing the right_pane reference
        self.left_pane = LeftPaneWidget(parent_di_testing_widget=self, right_pane_ref=self.right_pane)
        # self.left_pane.setStyleSheet("border: 1px solid gray; border-radius: 4px; padding: 2px;")
        
        splitter.addWidget(self.left_pane)
        splitter.addWidget(self.right_pane)

        total_width = self.parent().width() if self.parent() else 1600 # Use parent or default
        left_width = int(total_width * 0.20) 
        right_width = int(total_width * 0.80) 
        splitter.setSizes([left_width, right_width])
        splitter.setStretchFactor(0,0) 
        splitter.setStretchFactor(1,1)

        self.setLayout(main_layout)
        logger.info("DITestingWidget initialized with Left and Right Panes using QSplitter.")

    def closeEvent(self, event):
        logger.info("DITestingWidget closing.")
        super().closeEvent(event)