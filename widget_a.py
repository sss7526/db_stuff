from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton
from database_manager import ExampleTable1

class WidgetA(QWidget):
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        button = QPushButton("Query DB1 from Widget A")
        button.clicked.connect(self.query_db1)
        layout.addWidget(button)

        self.setLayout(layout)

    def query_db1(self):
        session = self.db_manager.get_session('database1')
        try:
            result = session.query(ExampleTable1).all()
            print([r.__dict__ for r in result])
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {e}")
        finally:
            session.close()