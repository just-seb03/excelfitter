import sys
import os
import mysql.connector
import socket
import csv
import json
from PySide6.QtCore import Qt, Signal, QThread
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QLineEdit,
                               QPushButton, QLabel, QFileDialog, QListWidget,
                               QMessageBox, QVBoxLayout, QComboBox,
                               QProgressBar, QDialog, QFormLayout, QFrame, QStyle)

CONFIG_FILE = "config.json"


def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_ip", "")
        except:
            return ""
    return ""


def save_config(ip):
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump({"last_ip": ip}, f)
    except:
        pass


def info():
    try:
        user_name = socket.gethostname()
        ip = socket.gethostbyname(user_name)
        return ip
    except:
        return "NO IP"


def pd_fitter(path_list, db_name=None, table_name=None, session_creds=None, check_local_dupes=False):
    try:
        combined_data = []
        first_columns = None
        for path in path_list:
            try:
                try:
                    with open(path, mode='r', encoding='utf-8-sig') as f:
                        lines = f.readlines()
                except UnicodeDecodeError:
                    with open(path, mode='r', encoding='latin-1') as f:
                        lines = f.readlines()
                if not lines: continue

                header_line = lines[0].strip()
                separator = ';' if header_line.count(';') > header_line.count(',') else ','
                current_columns = [c.strip().lower() for c in header_line.split(separator)]

                if first_columns is None:
                    first_columns = current_columns
                elif first_columns != current_columns:
                    raise Exception(f"Estructura incompatible en: {os.path.basename(path)}")

                for line in lines[1:]:
                    if not line.strip(): continue
                    parts = list(csv.reader([line.strip()], delimiter=separator))[0]
                    parts = (parts + [""] * len(current_columns))[:len(current_columns)]
                    combined_data.append(parts)
            except Exception as e:
                raise Exception(f"Error al leer {os.path.basename(path)}: {e}")

        if db_name and table_name and session_creds:
            try:
                conn = mysql.connector.connect(
                    host=session_creds["host"], user=session_creds["user"],
                    password=session_creds["pw"], database=db_name,
                    use_pure=True, auth_plugin='mysql_native_password'
                )
                cursor = conn.cursor(dictionary=True, buffered=True)
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
                db_cols = [str(c).strip().lower() for c in cursor.column_names]
                cursor.fetchall()

                if db_cols != first_columns:
                    cursor.close()
                    conn.close()
                    raise Exception(f"Las columnas no coinciden.\nDB: {db_cols}\nCSV: {first_columns}")

                if check_local_dupes:
                    cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
                    pk_results = cursor.fetchall()
                    if pk_results:
                        pk_col_name = pk_results[0]['Column_name'].lower()
                        if pk_col_name in first_columns:
                            pk_index = first_columns.index(pk_col_name)
                            vistos = set()
                            for row in combined_data:
                                val = str(row[pk_index]).strip()
                                if val in vistos:
                                    cursor.close()
                                    conn.close()
                                    raise Exception(f"Clave duplicada detectada en archivos: {val}")
                                vistos.add(val)
                cursor.close()
                conn.close()
            except mysql.connector.Error as e:
                raise Exception(f"Error de base de datos ({e.errno}): {e.msg}")

        return combined_data, first_columns
    except Exception as e:
        raise e


class LoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Credenciales de Base de Datos")
        self.setFixedSize(300, 150)
        layout = QFormLayout(self)
        self.user_input = QLineEdit()
        self.pass_input = QLineEdit()
        self.pass_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.btn_login = QPushButton("Validar y Conectar")
        self.btn_login.clicked.connect(self.accept)
        layout.addRow("Usuario:", self.user_input)
        layout.addRow("Contraseña:", self.pass_input)
        layout.addWidget(self.btn_login)

    def get_credentials(self):
        return self.user_input.text(), self.pass_input.text()


class UploadWorker(QThread):
    progreso = Signal(int, str)
    finalizado = Signal()
    error_senal = Signal(str)

    def __init__(self, path_list, db_name, table_name, session_creds):
        super().__init__()
        self.path_list = path_list
        self.db_name = db_name
        self.table_name = table_name
        self.session_creds = session_creds

    def run(self):
        try:
            self.progreso.emit(20, "Preparando datos...")
            data, headers = pd_fitter(self.path_list, self.db_name, self.table_name, self.session_creds, False)

            conn = mysql.connector.connect(
                host=self.session_creds["host"], user=self.session_creds["user"],
                password=self.session_creds["pw"], database=self.db_name,
                use_pure=True, auth_plugin='mysql_native_password'
            )
            cursor = conn.cursor()
            self.progreso.emit(60, "Subiendo registros...")
            placeholders = ", ".join(["%s"] * len(headers))
            cols_str = ", ".join([f"`{h}`" for h in headers])
            sql = f"INSERT INTO `{self.table_name}` ({cols_str}) VALUES ({placeholders})"

            try:
                cursor.executemany(sql, data)
                conn.commit()
            except mysql.connector.Error as e:
                error_str = str(e).lower()
                if "integrityerror" in error_str or "1062" in error_str or "duplicate" in error_str:
                    raise Exception("Existen claves primarias duplicadas\n Es imposible subir sus datos.")
                else:
                    raise Exception(f"Error desconocido: {error_str}")
            finally:
                cursor.close()
                conn.close()

            self.progreso.emit(100, "Carga exitosa.")
            self.finalizado.emit()
        except Exception as e:
            self.error_senal.emit(str(e))


class SendGui(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Procesando...")
        self.setFixedSize(450, 260)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        layout = QVBoxLayout()
        self.icon_label = QLabel()
        self.cambiar_icono(QStyle.StandardPixmap.SP_BrowserReload)
        self.status_label = QLabel("Iniciando...")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.barra = QProgressBar()
        self.barra.setRange(0, 0)
        self.btn_cerrar = QPushButton("Aceptar")
        self.btn_cerrar.setEnabled(False)
        self.btn_cerrar.clicked.connect(self.close)
        layout.addWidget(self.icon_label, alignment=Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)
        layout.addWidget(self.barra)
        layout.addWidget(self.btn_cerrar, alignment=Qt.AlignmentFlag.AlignCenter)
        self.setLayout(layout)

    def cambiar_icono(self, recurso):
        pixmap = self.style().standardIcon(recurso).pixmap(65, 65)
        self.icon_label.setPixmap(pixmap)

    def actualizar_interfaz(self, valor, texto):
        if valor > 0: self.barra.setRange(0, 100)
        self.barra.setValue(valor)
        self.status_label.setText(texto)

    def finalizar(self, exito, mensaje=""):
        self.btn_cerrar.setEnabled(True)
        self.barra.setRange(0, 100)
        if exito:
            self.status_label.setText("Completado con éxito.")
            self.cambiar_icono(QStyle.StandardPixmap.SP_DialogApplyButton)
            self.barra.setValue(100)
        else:
            self.cambiar_icono(QStyle.StandardPixmap.SP_MessageBoxCritical)
            self.status_label.setText(f"Error en el proceso:\n{mensaje}")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rutas_completas = []
        self.session_creds = None

        self.setWindowTitle("Excelfitter")
        self.setFixedSize(700, 310)
        self.mainWidget = QWidget()
        self.setCentralWidget(self.mainWidget)
        self.setWindowIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon))


        self.header_frame = QFrame(self.mainWidget)
        self.header_frame.setGeometry(0, 0, 700, 40)
        self.header_frame.setStyleSheet("background-color: #eef4fc;")

        estilo_header = """
            QPushButton { background: transparent; border: none; color: #1a4a7a; font-size: 13px; font-weight: bold; }
            QPushButton:hover { color: #2a6db0; }
        """

        self.download_button = QPushButton("Descargar", self.header_frame)
        self.download_button.setGeometry(20, 10, 80, 20)
        self.download_button.setStyleSheet(estilo_header)
        self.download_button.clicked.connect(self.request_download)

        self.send_button = QPushButton("Subir CSV", self.header_frame)
        self.send_button.setGeometry(110, 10, 80, 20)
        self.send_button.setStyleSheet(estilo_header)
        self.send_button.clicked.connect(self.send_action)

        self.fitter_button = QPushButton("Unir CSV", self.header_frame)
        self.fitter_button.setGeometry(200, 10, 80, 20)
        self.fitter_button.setStyleSheet(estilo_header)
        self.fitter_button.clicked.connect(self.run_pd_fitter_action)

        self.ip_input = QLineEdit(self.header_frame)
        self.ip_input.setPlaceholderText("IP Del Servidor")
        self.ip_input.setText(load_config())
        self.ip_input.setGeometry(450, 8, 130, 24)
        self.ip_input.setStyleSheet("border-radius: 5px; border: 1px solid #ccc; padding-left: 5px;")

        self.connect_btn = QPushButton("Conectar", self.header_frame)
        self.connect_btn.setGeometry(590, 8, 90, 24)
        self.connect_btn.setStyleSheet(
            "background-color: #1a4a7a; color: white; border-radius: 5px; font-weight: bold;")
        self.connect_btn.clicked.connect(self.attempt_connection)


        self.db_selector = QComboBox(self.mainWidget)
        self.db_selector.setPlaceholderText("Seleccione DB")
        self.db_selector.setGeometry(30, 60, 150, 30)
        self.db_selector.currentIndexChanged.connect(self.cargar_tablas)

        self.table_selector = QComboBox(self.mainWidget)
        self.table_selector.setPlaceholderText("Seleccione Tabla")
        self.table_selector.setGeometry(190, 60, 380, 30)

        self.browse_button = QPushButton("Explorar", self.mainWidget)
        self.browse_button.setGeometry(580, 60, 90, 30)
        self.browse_button.clicked.connect(self.browse_action)

        self.csv_list = QListWidget(self.mainWidget)
        self.csv_list.setGeometry(30, 100, 640, 80)
        self.csv_list.setSelectionMode(QListWidget.ExtendedSelection)

        self.importante_label = QLabel(self.mainWidget)
        self.importante_label.setText(
            "<b>Importante:</b> Verifique que los documentos tengan el número exacto de columnas y nombres que la tabla a la cual hara la inserccion de sus datos.")
        self.importante_label.setGeometry(30, 185, 640, 40)
        self.importante_label.setWordWrap(True)


        self.footer_frame = QFrame(self.mainWidget)
        self.footer_frame.setGeometry(0, 240, 700, 70)
        self.footer_frame.setStyleSheet("background-color: #eef4fc;")
        self.footer_frame.lower()

        self.network_icon = QLabel(self.mainWidget)
        self.network_icon.setGeometry(30, 250, 50, 50)
        self.network_icon.setPixmap(self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon).pixmap(40, 40))

        self.ip_label = QLabel(f"Tu Equipo: {info()}", self.mainWidget)
        self.ip_label.setGeometry(80, 255, 300, 16)

        self.status_label = QLabel("Estado: Sin iniciar", self.mainWidget)
        self.status_label.setGeometry(80, 275, 300, 16)

    def attempt_connection(self):
        target_ip = self.ip_input.text().strip()
        if not target_ip:
            QMessageBox.warning(self, "Aviso", "Ingrese la IP del servidor.")
            return

        login = LoginDialog(self)
        if login.exec() == QDialog.DialogCode.Accepted:
            user, pw = login.get_credentials()
            self.status_label.setText("Estado: Conectando...")
            self.status_label.setStyleSheet("color: black;")
            QApplication.processEvents()

            try:
                conn = mysql.connector.connect(
                    host=target_ip, user=user, password=pw,
                    use_pure=True, auth_plugin='mysql_native_password', connect_timeout=4
                )
                if conn.is_connected():
                    self.session_creds = {"host": target_ip, "user": user, "pw": pw}
                    save_config(target_ip)
                    cursor = conn.cursor()
                    cursor.execute("SHOW DATABASES")
                    db_list = [db[0] for db in cursor]

                    for sys_db in ["information_schema", "mysql", "performance_schema", "phpmyadmin", "test"]:
                        if sys_db in db_list: db_list.remove(sys_db)

                    self.db_selector.clear()
                    self.db_selector.addItems(db_list)
                    self.status_label.setText(f"Estado: Conectado como {user}")
                    self.status_label.setStyleSheet("color: green;")
                    conn.close()
            except mysql.connector.Error as e:
                self.session_creds = None
                self.status_label.setText("Estado: Error de conexión")
                self.status_label.setStyleSheet("color: red;")
                QMessageBox.critical(self, "Error", f"No se pudo conectar: {e.msg}")

    def cargar_tablas(self):
        db_name = self.db_selector.currentText()
        if not db_name or not self.session_creds: return

        self.table_selector.clear()
        try:
            conn = mysql.connector.connect(
                host=self.session_creds["host"], user=self.session_creds["user"],
                password=self.session_creds["pw"], database=db_name,
                use_pure=True, auth_plugin='mysql_native_password'
            )
            cursor = conn.cursor()
            cursor.execute("SHOW FULL TABLES")
            for name, table_type in cursor:
                label = f"[VISTA] {name}" if table_type == 'VIEW' else name
                self.table_selector.addItem(label)
            if self.table_selector.count() == 0:
                self.table_selector.setPlaceholderText("No se encontraron tablas")
            conn.close()
        except Exception as e:
            print(f"Error al cargar tablas: {e}")

    def browse_action(self):
        rutas, _ = QFileDialog.getOpenFileNames(self, "Seleccionar CSV", os.getcwd(), "Archivos CSV (*.csv)")
        for r in rutas:
            if not self.csv_list.findItems(os.path.basename(r), Qt.MatchFlag.MatchExactly):
                self.csv_list.addItem(os.path.basename(r))
                self.rutas_completas.append(r)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Delete:
            indices = [i.row() for i in self.csv_list.selectedIndexes()]
            for index in sorted(indices, reverse=True):
                self.csv_list.takeItem(index)
                if index < len(self.rutas_completas):
                    self.rutas_completas.pop(index)
        else:
            super().keyPressEvent(event)

    def request_download(self):
        db = self.db_selector.currentText()
        table_raw = self.table_selector.currentText()
        if not db or not table_raw or not self.session_creds:
            QMessageBox.warning(self, "Aviso", "Debe conectarse y seleccionar una tabla primero.")
            return

        table = table_raw.replace("[VISTA] ", "")
        path_save, _ = QFileDialog.getSaveFileName(self, "Guardar Datos", f"{table}.csv", "CSV Files (*.csv)")

        if path_save:
            try:
                conn = mysql.connector.connect(
                    host=self.session_creds["host"], user=self.session_creds["user"],
                    password=self.session_creds["pw"], database=db,
                    use_pure=True, auth_plugin='mysql_native_password'
                )
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM `{table}`")
                headers = [i[0] for i in cursor.description]
                rows = cursor.fetchall()
                with open(path_save, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f, delimiter=',')
                    writer.writerow(headers)
                    writer.writerows(rows)
                conn.close()
                QMessageBox.information(self, "Éxito", f"Tabla '{table}' descargada correctamente.")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo descargar: {str(e)}")

    def send_action(self):
        raw_table = self.table_selector.currentText()
        db = self.db_selector.currentText()

        if not self.session_creds:
            QMessageBox.warning(self, "Aviso", "Primero debe conectarse al servidor.")
            return
        if "[VISTA]" in raw_table:
            QMessageBox.warning(self, "Acción no permitida",
                                "No se pueden subir datos a una Vista. Seleccione una tabla base.")
            return
        if not db or not raw_table or self.csv_list.count() == 0:
            QMessageBox.warning(self, "Aviso",
                                "Asegúrese de seleccionar DB, Tabla y tener al menos un archivo cargado en la lista.")
            return

        self.send_gui = SendGui()
        self.send_gui.show()
        self.worker = UploadWorker(self.rutas_completas, db, raw_table, self.session_creds)
        self.worker.progreso.connect(self.send_gui.actualizar_interfaz)
        self.worker.finalizado.connect(lambda: self.send_gui.finalizar(True))
        self.worker.error_senal.connect(lambda m: self.send_gui.finalizar(False, m))
        self.worker.start()

    def run_pd_fitter_action(self):
        if self.csv_list.count() == 0:
            QMessageBox.warning(self, "Aviso", "Agregue archivos CSV a la lista primero.")
            return

        resp = QMessageBox.question(self, "Unir", "¿Desea verificar Estructura y Claves duplicadas contra la DB?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        verify = (resp == QMessageBox.StandardButton.Yes)

        db = None
        table = None

        if verify:
            if not self.session_creds:
                QMessageBox.warning(self, "Aviso", "Para verificar contra la DB, debe conectarse al servidor primero.")
                return
            db = self.db_selector.currentText()
            table = self.table_selector.currentText().replace("[VISTA] ", "")
            if not db or not table:
                QMessageBox.warning(self, "Aviso", "Seleccione DB y Tabla para realizar la verificación.")
                return

        try:
            data, headers = pd_fitter(self.rutas_completas, db, table, self.session_creds, verify)
            path_save, _ = QFileDialog.getSaveFileName(self, "Guardar", "unificado.csv", "CSV Files (*.csv)")
            if path_save:
                with open(path_save, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f, delimiter=',')
                    writer.writerow(headers)
                    writer.writerows(data)
                QMessageBox.information(self, "Éxito", "CSV unificado correctamente.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("windowsvista")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())