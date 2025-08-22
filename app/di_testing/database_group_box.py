import logging
from PyQt5.QtWidgets import QLineEdit , QLabel , QComboBox , QGridLayout
from PyQt5.QtCore import Qt , QSettings
from shared_widgets.collapsible_group_box import CollapsibleGroupBox

logger = logging.getLogger(__name__)

class DatabaseGroupBox(CollapsibleGroupBox) :
    def __init__(self , parent=None) :
        super().__init__("Database Configuration" , parent)
        self.initUI()
        self.loadSettings() 
        logger.debug("DatabaseGroupBox initialized.")

    def initUI(self) :
        layout = QGridLayout()
        layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        layout.setHorizontalSpacing(10)
        layout.setVerticalSpacing(10)
        fields = [
            ('DB Username:' , QLineEdit()) , ('DB Password:' , QLineEdit()) ,
            ('DB Host:' , QLineEdit()) , ('DB Name:' , QLineEdit()) ,
            ('DB Driver:' , QComboBox()) , ('DB URL:' , QLineEdit())
        ]
        self.widgets = {}
        for i , (label_text , widget) in enumerate(fields) :
            layout.addWidget(QLabel(label_text) , i , 0 , alignment=Qt.AlignLeft | Qt.AlignVCenter)
            layout.addWidget(widget , i , 1 , alignment=Qt.AlignLeft | Qt.AlignVCenter)
            key = label_text.strip(':') 
            self.widgets[key] = widget
            if label_text == 'DB Password:' : widget.setEchoMode(QLineEdit.Password)
            elif label_text == 'DB Driver:' : widget.addItems(['Postgres', 'MS SQL']) 
            elif label_text == 'DB URL:' : widget.setReadOnly(True); widget.setToolTip("Generated automatically based on other DB fields.")
            if isinstance(widget , QLineEdit) and label_text != 'DB URL:' : widget.textChanged.connect(self.updateDbUrl)
            elif isinstance(widget , QComboBox) : widget.currentIndexChanged.connect(self.updateDbUrl)
        
        layout.setColumnStretch(0, 0) 
        layout.setColumnStretch(1, 1) 

        self.setContentLayout(layout)
        self.updateDbUrl() 
            
    def updateDbUrl(self) :
        user = self.widgets['DB Username'].text()
        password = self.widgets['DB Password'].text() 
        host = self.widgets['DB Host'].text()
        dbname = self.widgets['DB Name'].text()
        driver = self.widgets['DB Driver'].currentText()
        conn_string = self.generate_db_url(user , password , host , dbname , driver)
        self.widgets['DB URL'].setText(conn_string)

    @staticmethod
    def generate_db_url(user , password , host , dbname , driver) :
        if not all([host, dbname]): return "Please fill in Host and DB Name"
        if driver == 'Postgres' : return f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
        elif driver == 'MS SQL' : return f"mssql+pyodbc://{user}:{password}@{host}/{dbname}?driver=ODBC+Driver+11+for+SQL+Server" 
        return "Unsupported DB Driver"

    def loadSettings(self) :
        settings = QSettings("S&P Global" , "ClariFI DI Validator")
        self.widgets['DB Username'].setText(settings.value("dbUsername" , ""))
        self.widgets['DB Password'].setText(settings.value("dbPassword" , "")) 
        self.widgets['DB Host'].setText(settings.value("dbHost" , ""))
        self.widgets['DB Name'].setText(settings.value("dbName" , ""))
        self.widgets['DB Driver'].setCurrentText(settings.value("dbDriver" , "Postgres"))
        self.updateDbUrl() 
        logger.info("Database settings loaded.")

    def saveSettings(self) :
        settings = QSettings("S&P Global" , "ClariFI DI Validator")
        settings.setValue("dbUsername" , self.widgets['DB Username'].text() or None)
        settings.setValue("dbPassword" , self.widgets['DB Password'].text() or None) 
        settings.setValue("dbHost" , self.widgets['DB Host'].text() or None)
        settings.setValue("dbName" , self.widgets['DB Name'].text() or None)
        settings.setValue("dbDriver" , self.widgets['DB Driver'].currentText())
        logger.info("Database settings saved.")

    def get_settings(self):
        return {
            "db_username": self.widgets['DB Username'].text() or None,
            "db_password": self.widgets['DB Password'].text() or None, 
            "db_host": self.widgets['DB Host'].text() or None,
            "db_name": self.widgets['DB Name'].text() or None,
            "db_driver": self.widgets['DB Driver'].currentText(),
            "db_url": self.widgets['DB URL'].text() or None
        }