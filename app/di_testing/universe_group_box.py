import logging
from PyQt5.QtWidgets import QLineEdit , QLabel , QComboBox , QGridLayout
from PyQt5.QtCore import Qt, QSettings
from shared_widgets.collapsible_group_box import CollapsibleGroupBox

logger = logging.getLogger(__name__)

class UniverseGroupBox(CollapsibleGroupBox) :
    def __init__(self , parent=None) :
        super().__init__("Universe and API Configuration" , parent)
        self.initUI()
        self.loadSettings()
        logger.debug("UniverseGroupBox initialized.")

    def initUI(self):
        layout = QGridLayout()
        layout.setAlignment(Qt.AlignTop | Qt.AlignLeft) 

        fields = [
            ('API Username:', QLineEdit()),
            ('API Password:', QLineEdit()),
            ('Universe Name:' , QLineEdit()),
            ('Universe Type:' , QComboBox())
        ]
        self.widgets = {}

        for i, (label_text, widget) in enumerate(fields):
            layout.addWidget(QLabel(label_text), i, 0, alignment=Qt.AlignLeft | Qt.AlignVCenter)
            layout.addWidget(widget, i, 1, alignment=Qt.AlignLeft | Qt.AlignVCenter)
            
            if label_text == 'API Password:' :
                widget.setEchoMode(QLineEdit.Password)
            if label_text == 'Universe Type:':
                widget.addItems(['basket', 'portfolio'])
            self.widgets[label_text.strip(':')] = widget 
        
        layout.setColumnStretch(0, 0) 
        layout.setColumnStretch(1, 1) 

        self.setContentLayout(layout)

    def loadSettings(self):
        settings = QSettings("S&P Global" , "ClariFI DI Validator")
        self.widgets['API Username'].setText(settings.value("apiUsername", "clarifi"))
        self.widgets['API Password'].setText(settings.value("apiPassword", "clarifi"))
        self.widgets['Universe Name'].setText(settings.value("universeName", ""))
        self.widgets['Universe Type'].setCurrentText(settings.value("universeType", "basket"))
        logger.info("Universe settings loaded.")

    def saveSettings(self):
        settings = QSettings("S&P Global" , "ClariFI DI Validator")
        settings.setValue("apiUsername", self.widgets['API Username'].text())
        settings.setValue("apiPassword", self.widgets['API Password'].text())
        settings.setValue("universeName", self.widgets['Universe Name'].text())
        settings.setValue("universeType", self.widgets['Universe Type'].currentText())
        logger.info("Universe settings saved.")

    def get_settings(self):
        return {
            "api_username": self.widgets['API Username'].text(),
            "api_password": self.widgets['API Password'].text(), 
            "universe_name": self.widgets['Universe Name'].text(),
            "universe_type": self.widgets['Universe Type'].currentText()
        }