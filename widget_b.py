from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton
from database_manager import ExampleTable2

class WidgetB(QWidget):
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        button = QPushButton("Query DB2 from Widget B")
        button.clicked.connect(self.query_db2)
        layout.addWidget(button)

        self.setLayout(layout)

    def query_db2(self):
        session = self.db_manager.get_session('database2')
        result = session.query(ExampleTable2).all()
        session.close()
        print([r.__dict__ for r in result])