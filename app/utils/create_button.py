from PyQt5.QtWidgets import QPushButton, QSizePolicy


def createButton(text , slot , enabled=True, tooltip=None) :
    button = QPushButton(text)
    
    button.setEnabled(enabled)
    button.clicked.connect(slot)
    button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
    # button.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)
    # button.setMinimumWidth(100)
    if tooltip:
       button.setToolTip(tooltip)
    return button