import pandas as pd
import os
import re
import warnings
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
from datetime import datetime

warnings.filterwarnings('ignore')


class ProgressWindow:
    """Окно прогресса с прогресс-баром и логом"""

    def __init__(self):
        self.root = tk.Toplevel()
        self.root.title("Обработка данных")
        self.root.geometry("600x400")
        self.root.resizable(True, True)

        # Прогресс-бар
        self.progress_label = ttk.Label(self.root, text="Подготовка к обработке...")
        self.progress_label.pack(pady=5)

        self.progress = ttk.Progressbar(self.root, orient='horizontal', length=500, mode='determinate')
        self.progress.pack(pady=10, padx=20, fill='x')

        # Текстовое поле для логов
        self.log_text = scrolledtext.ScrolledText(self.root, height=15, width=70)
        self.log_text.pack(pady=10, padx=20, fill='both', expand=True)

        # Кнопка отмены
        self.cancel_button = ttk.Button(self.root, text="Отмена", command=self.cancel)
        self.cancel_button.pack(pady=5)

        self.is_cancelled = False

    def log(self, message):
        """Добавление сообщения в лог"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.root.update()

    def update_progress(self, value, message=None):
        """Обновление прогресс-бара"""
        self.progress['value'] = value
        if message:
            self.progress_label.config(text=message)
        self.root.update()

    def cancel(self):
        """Отмена операции"""
        self.is_cancelled = True
        self.log("Операция отменена пользователем")

    def close(self):
        """Закрытие окна"""
        self.root.destroy()


class DataProcessor:
    """Класс для обработки данных с GUI"""

    def __init__(self):
        self.progress_window = None
        self.current_folder = os.getcwd()

    def log(self, message):
        """Логирование сообщения"""
        if self.progress_window:
            self.progress_window.log(message)
        print(message)

    def update_progress(self, value, message=None):
        """Обновление прогресса"""
        if self.progress_window:
            self.progress_window.update_progress(value, message)

    def check_cancelled(self):
        """Проверка отмены операции"""
        if self.progress_window and self.progress_window.is_cancelled:
            raise Exception("Операция отменена пользователем")

    def select_folder(self):
        """Выбор папки через диалоговое окно"""
        folder = filedialog.askdirectory(initialdir=self.current_folder, title="Выберите папку с файлами")
        if folder:
            self.current_folder = folder
            return folder
        return None

    def find_files_by_pattern(self, folder_path):
        """Поиск файлов по шаблонам"""
        files = os.listdir(folder_path)

        id_pattern = re.compile(r'Исполнительная документация от.*\.xlsx$', re.IGNORECASE)
        structure_pattern = re.compile(r'Структура объекта.*\.xlsx$', re.IGNORECASE)

        id_files = [f for f in files if id_pattern.match(f)]
        structure_files = [f for f in files if structure_pattern.match(f)]

        return id_files, structure_files

    def clean_dataframe_columns(self, df):
        """Очистка и подготовка колонок DataFrame"""
        # Если DataFrame имеет MultiIndex в колонках, преобразуем его в обычные колонки
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.columns = [str(col).strip() for col in df.columns]

        # Проверим на дубликаты колонок и удалим их
        if df.columns.duplicated().any():
            self.log("ВНИМАНИЕ: Обнаружены дубликаты в названиях колонок! Удаляем дубликаты...")

            new_columns = []
            seen_columns = set()

            for col in df.columns:
                if col not in seen_columns:
                    new_columns.append(col)
                    seen_columns.add(col)
                else:
                    counter = 1
                    new_col_name = f"{col}_dup{counter}"
                    while new_col_name in seen_columns:
                        counter += 1
                        new_col_name = f"{col}_dup{counter}"
                    new_columns.append(new_col_name)
                    seen_columns.add(new_col_name)

            df.columns = new_columns

        return df

    def normalize_text(self, text):
        """Нормализация текста для лучшего сопоставления"""
        if not isinstance(text, str):
            return ""

        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s]', '', text)

        return text

    def load_id_data(self, folder_path, id_filename):
        """Загрузка данных из файла исполнительной документации с листа 'Элементы_структуры_в_ИД'"""
        self.check_cancelled()
        id_file_path = os.path.join(folder_path, id_filename)

        self.log(f"Загрузка файла ИД: {id_filename}")

        try:
            # Сначала попробуем прочитать лист "Элементы_структуры_в_ИД"
            self.log("Попытка загрузки с листа 'Элементы_структуры_в_ИД'...")

            try:
                # Пробуем загрузить с нужного листа
                temp_df = pd.read_excel(id_file_path, sheet_name='Элементы_структуры_в_ИД', nrows=10)
                sheet_name = 'Элементы_структуры_в_ИД'
                self.log("✓ Лист 'Элементы_структуры_в_ИД' найден")
            except Exception as e:
                self.log("✗ Лист 'Элементы_структуры_в_ИД' не найден, пробуем первый лист")
                # Если лист не найден, пробуем первый лист
                temp_df = pd.read_excel(id_file_path, nrows=10)
                sheet_name = 0  # первый лист

            # Определяем строку заголовков
            header_row = None
            for i in range(min(5, len(temp_df))):
                row_values = temp_df.iloc[i].values
                row_str = ' '.join([str(x) for x in row_values if pd.notna(x)])
                if any(keyword in row_str.lower() for keyword in
                       ['шифр', 'конструктив', 'вид работ', 'объем', 'work', 'id', 'document']):
                    header_row = i
                    self.log(f"Найден заголовок ИД в строке {i + 1}")
                    break

            if header_row is None:
                self.log("Заголовки ИД не найдены, используем первую строку")
                header_row = 0
            else:
                self.log(f"Используем строку {header_row + 1} как заголовок ИД")

            # Загружаем данные с правильным заголовком и нужного листа
            df = pd.read_excel(id_file_path, sheet_name=sheet_name, header=header_row)

            if df.empty:
                self.log("✗ Файл ИД пустой")
                return None

            df = self.clean_dataframe_columns(df)

            self.log("Все доступные колонки в ИД:")
            for i, col in enumerate(df.columns):
                self.log(f"  {i + 1}. '{col}'")

            # Маппинг колонок - только первое вхождение для каждого целевого имени
            column_mapping = {}
            target_names = ['Объем_по_документу', 'Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ']

            for col in df.columns:
                col_name = str(col).lower()

                # ДИАГНОСТИКА: выведем оригинальное название и его нижний регистр для отладки
                self.log(f"  Проверка колонки: '{col}' -> '{col_name}'")

                # ОБЪЕМ ПО ДОКУМЕНТУ - ДОЛЖЕН БЫТЬ ПЕРВЫМ (самое специфичное условие)
                if 'Объем_по_документу' not in column_mapping.values():
                    volume_condition1 = 'объём по документу' in col_name and 'work scopes' in col_name
                    volume_condition2 = 'объем по документу' in col_name and 'work scopes' in col_name

                    if volume_condition1 or volume_condition2:
                        column_mapping[col] = 'Объем_по_документу'
                        self.log(f"  ✓ Найдена колонка для Объем_по_документу: '{col}'")
                        continue  # переходим к следующей колонке

                # Шифр должен браться из колонки "Шифры комплектов РД / Detailed Design Documents Sections"
                if 'Шифр_комплекта' not in column_mapping.values():
                    if (('шифры комплектов рд' in col_name and 'detailed design documents sections' in col_name) or
                            ('шифры комплектов' in col_name and 'detailed design' in col_name) or
                            'шифры комплектов рд' in col_name or
                            'detailed design documents sections' in col_name or
                            ('шифр' in col_name and 'комплект' in col_name and 'рд' in col_name)):
                        column_mapping[col] = 'Шифр_комплекта'
                        self.log(f"  Найдена колонка для Шифр_комплекта: '{col}'")
                        continue

                # Конструктивный элемент
                if 'Конструктивный_элемент' not in column_mapping.values():
                    if (('конструктив' in col_name or 'construction' in col_name or
                         'элемент' in col_name or 'element' in col_name or
                         'конструкц' in col_name or 'конструкции' in col_name)):
                        column_mapping[col] = 'Конструктивный_элемент'
                        self.log(f"  Найдена колонка для Конструктивный_элемент: '{col}'")
                        continue

                # Вид работ - САМОЕ ОБЩЕЕ УСЛОВИЕ (должно быть последним)
                if 'Вид_работ' not in column_mapping.values():
                    if (('вид работ' in col_name or 'works' in col_name or 'work' in col_name or
                         'type of work' in col_name or 'вид_работ' in col_name or
                         'type of' in col_name and 'work' in col_name)):
                        column_mapping[col] = 'Вид_работ'
                        self.log(f"  Найдена колонка для Вид_работ: '{col}'")
                        continue

            self.log(f"Найдено колонок для маппинга в ИД: {len(column_mapping)}")
            for orig, new in column_mapping.items():
                self.log(f"  '{orig}' -> '{new}'")

            # Переименовываем колонки
            df = df.rename(columns=column_mapping)

            # Очищаем от дубликатов колонок после переименования
            df = self.clean_dataframe_columns(df)

            # Оставляем только нужные колонки
            needed_cols = ['Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ', 'Объем_по_документу']
            available_cols = [col for col in needed_cols if col in df.columns]

            self.log(f"Доступные колонки после маппинга: {available_cols}")
            self.log(f"Отсутствующие колонки: {[col for col in needed_cols if col not in available_cols]}")

            # Если не хватает критически важных колонок
            if 'Шифр_комплекта' not in available_cols or 'Объем_по_документу' not in available_cols or len(
                    available_cols) < 2:
                self.log("Недостаточно колонок в файле ИД")
                return None

            df = df[available_cols]

            # Очистка данных
            for col in df.columns:
                try:
                    if col in ['Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ']:
                        # Для строковых колонок
                        if hasattr(df[col], 'fillna'):
                            df[col] = df[col].fillna('').astype(str)
                        else:
                            # Если это не pandas Series, преобразуем в список и затем в Series
                            values = [str(x) if pd.notna(x) else '' for x in df[col]]
                            df[col] = pd.Series(values, index=df.index)

                        # Убедимся, что это Series перед применением .str
                        if isinstance(df[col], pd.Series):
                            df[col] = df[col].str.strip()
                        else:
                            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

                        non_empty_count = (df[col] != '').sum()
                        self.log(
                            f"Обработана строковая колонка '{col}': {len(df[col])} значений, непустых: {non_empty_count}")

                    elif col == 'Объем_по_документу':
                        # Для числовой колонки
                        if hasattr(df[col], 'fillna'):
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        else:
                            # Если это не pandas Series, преобразуем в список и затем в Series
                            values = []
                            for x in df[col]:
                                try:
                                    values.append(float(x) if pd.notna(x) else 0)
                                except (ValueError, TypeError):
                                    values.append(0)
                            df[col] = pd.Series(values, index=df.index)

                        non_zero_count = (df[col] > 0).sum()
                        self.log(
                            f"Обработана числовая колонка '{col}': {len(df[col])} значений, из них ненулевых: {non_zero_count}")

                except Exception as col_error:
                    self.log(f"Ошибка при обработке колонки '{col}': {str(col_error)}")
                    # Продолжаем обработку других колонок

            # Фильтруем данные: удаляем строки с пустыми ключевыми полями
            initial_count = len(df)

            # Безопасная фильтрация - убедимся, что работаем с Series
            mask = pd.Series([True] * len(df), index=df.index)

            for col in ['Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ']:
                if col in df.columns:
                    if isinstance(df[col], pd.Series):
                        col_series = df[col]
                    else:
                        # Если это не Series, преобразуем
                        col_series = pd.Series(df[col].iloc[:, 0] if hasattr(df[col], 'iloc') else df[col],
                                               index=df.index)

                    # Применяем фильтрацию
                    if hasattr(col_series, 'str'):
                        mask = mask & (col_series.str.strip() != '')
                    else:
                        # Альтернативный способ для не-строковых данных
                        mask = mask & (col_series.astype(str).str.strip() != '')

            df = df[mask].copy()
            filtered_count = len(df)

            self.log(f"Отфильтровано записей ИД: {initial_count} -> {filtered_count}")
            self.log(f"✓ Успешно загружено {len(df)} записей из ИД")

            # Выводим примеры данных для проверки
            if len(df) > 0:
                self.log("Примеры загруженных данных ИД:")
                for i in range(min(3, len(df))):
                    row = df.iloc[i]
                    self.log(
                        f"  Запись {i + 1}: Шифр='{row['Шифр_комплекта']}', Конструкция='{row['Конструктивный_элемент']}', Вид работ='{row['Вид_работ']}', Объем={row['Объем_по_документу']}")

            return df

        except Exception as e:
            self.log(f"✗ Ошибка при загрузке файла ИД: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def load_structure_data(self, folder_path, structure_filename):
        """Загрузка данных из файла структуры объекта"""
        self.check_cancelled()
        structure_file_path = os.path.join(folder_path, structure_filename)

        self.log(f"Загрузка файла структуры: {structure_filename}")

        try:
            # Определяем строку заголовков
            temp_df = pd.read_excel(structure_file_path, sheet_name='Виды работ', nrows=10)

            header_row = None
            for i in range(min(5, len(temp_df))):
                row_values = temp_df.iloc[i].values
                row_str = ' '.join([str(x) for x in row_values if pd.notna(x)])
                if any(keyword in row_str.lower() for keyword in ['вид работ', 'проект', 'марка', 'конструкция']):
                    header_row = i
                    break

            if header_row is None:
                header_row = 0

            df = pd.read_excel(structure_file_path, sheet_name='Виды работ', header=header_row)

            if df.empty:
                self.log("✗ Лист 'Виды работ' пустой")
                return None

            self.log(f"Загружено {len(df)} строк с листа 'Виды работ'")

            df = self.clean_dataframe_columns(df)

            # Маппинг колонок
            column_mapping = {}

            for col in df.columns:
                col_name = str(col).lower()

                if 'проект' in col_name and 'Проект' not in column_mapping.values():
                    column_mapping[col] = 'Проект'
                elif ('марка' in col_name or 'шифр' in col_name) and 'Марка' not in column_mapping.values():
                    column_mapping[col] = 'Марка'
                elif 'конструкция' in col_name and 'Конструкция' not in column_mapping.values():
                    column_mapping[col] = 'Конструкция'
                elif 'вид работ' in col_name and 'Вид_работ_по_классификатору' not in column_mapping.values():
                    column_mapping[col] = 'Вид_работ_по_классификатору'
                elif 'ед. изм' in col_name and 'Ед_изм' not in column_mapping.values():
                    column_mapping[col] = 'Ед_изм'
                elif 'проектный объем' in col_name and 'Проектный_объем' not in column_mapping.values():
                    column_mapping[col] = 'Проектный_объем'
                elif 'выполнено в натуре' in col_name and 'Выполнено_в_натуре' not in column_mapping.values():
                    column_mapping[col] = 'Выполнено_в_натуре'
                elif 'принято по rfi' in col_name and 'Принято_по_RFI' not in column_mapping.values():
                    column_mapping[col] = 'Принято_по_RFI'

            df = df.rename(columns=column_mapping)

            # Проверяем наличие необходимых колонок
            required_cols = ['Проект', 'Марка', 'Конструкция', 'Вид_работ_по_классификатору', 'Ед_изм']
            available_cols = [col for col in required_cols if col in df.columns]

            if len(available_cols) < 3:
                self.log(f"✗ Недостаточно обязательных колонок на листе 'Виды работ'")
                return None

            # Очистка данных
            for col in df.columns:
                if col in ['Проект', 'Марка', 'Конструкция', 'Вид_работ_по_классификатору', 'Ед_изм']:
                    df[col] = df[col].fillna('').astype(str).str.strip()
                elif col in ['Проектный_объем', 'Выполнено_в_натуре', 'Принято_по_RFI']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            self.log(f"✓ Успешно загружено {len(df)} записей из структуры")
            return df

        except Exception as e:
            self.log(f"✗ Ошибка при загрузке листа 'Виды работ': {str(e)}")
            return None

    def create_pivot_table(self, structure_df):
        """Создание сводной таблицы из структуры"""
        self.check_cancelled()
        self.log("Создание сводной таблицы из структуры...")

        required_cols = ['Проект', 'Марка', 'Конструкция', 'Вид_работ_по_классификатору', 'Ед_изм']
        missing_cols = [col for col in required_cols if col not in structure_df.columns]

        if missing_cols:
            self.log(f"✗ Отсутствуют необходимые колонки: {missing_cols}")
            return None

        # Убедимся, что числовые колонки существуют
        numeric_cols = ['Проектный_объем', 'Выполнено_в_натуре', 'Принято_по_RFI']
        for col in numeric_cols:
            if col not in structure_df.columns:
                structure_df[col] = 0

        # Обработка колонок для группировки
        groupby_cols = ['Проект', 'Марка', 'Конструкция', 'Вид_работ_по_классификатору', 'Ед_изм']
        for col in groupby_cols:
            if col in structure_df.columns:
                structure_df[col] = structure_df[col].astype(str).str.strip()

        # Группируем данные
        try:
            pivot_df = structure_df.groupby(groupby_cols, as_index=False).agg({
                'Проектный_объем': 'sum',
                'Выполнено_в_натуре': 'sum',
                'Принято_по_RFI': 'sum'
            })

            pivot_df['Подтверждено_ИД'] = 0

            self.log(f"✓ Создана сводная таблица с {len(pivot_df)} записями")
            return pivot_df

        except Exception as e:
            self.log(f"✗ Ошибка при создании сводной таблицы: {str(e)}")
            return None

    def optimize_matching(self, pivot_df, id_df):
        """Оптимизированное объединение данных по трём условиям"""
        self.check_cancelled()
        self.log("Оптимизированное объединение данных по трём условиям...")

        required_id_cols = ['Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ', 'Объем_по_документу']
        missing_id_cols = [col for col in required_id_cols if col not in id_df.columns]

        if missing_id_cols:
            self.log(f"✗ В ИД отсутствуют необходимые колонки: {missing_id_cols}")
            return pivot_df

        try:
            self.log("Подготовка данных...")
            self.update_progress(10, "Подготовка данных")

            # Создаем копии для безопасности
            structure_df = pivot_df.copy()
            id_df_work = id_df.copy()

            # Очищаем данные - более тщательная очистка
            for col in ['Проект', 'Конструкция', 'Вид_работ_по_классификатору']:
                if col in structure_df.columns:
                    structure_df[col] = structure_df[col].fillna('').astype(str).str.strip()

            for col in ['Шифр_комплекта', 'Конструктивный_элемент', 'Вид_работ']:
                if col in id_df_work.columns:
                    id_df_work[col] = id_df_work[col].fillna('').astype(str).str.strip()

            # Удаляем строки ИД с пустыми ключевыми полями
            id_df_work = id_df_work[
                (id_df_work['Шифр_комплекта'] != '') &
                (id_df_work['Конструктивный_элемент'] != '') &
                (id_df_work['Вид_работ'] != '')
                ].copy()

            self.log(f"Обрабатывается {len(id_df_work)} записей ИД и {len(structure_df)} записей структуры")

            # ДЕТАЛЬНАЯ ОТЛАДКА - выводим примеры данных
            self.log("\nПримеры данных для отладки:")
            self.log("Первые 3 записи ИД:")
            for i in range(min(3, len(id_df_work))):
                row = id_df_work.iloc[i]
                self.log(
                    f"  ИД {i + 1}: Шифр='{row['Шифр_комплекта']}', Конструкция='{row['Конструктивный_элемент']}', Вид работ='{row['Вид_работ']}'")

            self.log("Первые 3 записи структуры:")
            for i in range(min(3, len(structure_df))):
                row = structure_df.iloc[i]
                self.log(
                    f"  Структура {i + 1}: Проект='{row['Проект']}', Конструкция='{row['Конструкция']}', Вид работ='{row['Вид_работ_по_классификатору']}'")

            # Создаем составные ключи для быстрого сопоставления
            self.log("Создание ключей для сопоставления...")
            self.update_progress(30, "Создание ключей")

            # Создаем ключи для структуры
            structure_df['composite_key'] = (
                    structure_df['Проект'].str.strip() + "|||" +
                    structure_df['Конструкция'].str.strip() + "|||" +
                    structure_df['Вид_работ_по_классификатору'].str.strip()
            )

            # Создаем ключи для ИД и группируем по ним, суммируя объемы
            id_df_work['composite_key'] = (
                    id_df_work['Шифр_комплекта'].str.strip() + "|||" +
                    id_df_work['Конструктивный_элемент'].str.strip() + "|||" +
                    id_df_work['Вид_работ'].str.strip()
            )

            # Выводим примеры ключей для отладки
            self.log("\nПримеры составных ключей:")
            self.log("ИД ключи:")
            for i in range(min(3, len(id_df_work))):
                self.log(f"  {id_df_work.iloc[i]['composite_key']}")

            self.log("Структура ключи:")
            for i in range(min(3, len(structure_df))):
                self.log(f"  {structure_df.iloc[i]['composite_key']}")

            # Группируем ИД по ключам и суммируем объемы
            id_grouped = id_df_work.groupby('composite_key')['Объем_по_документу'].sum().reset_index()

            self.log(f"Создано {len(id_grouped)} уникальных комбинаций из ИД")

            # Создаем словарь для быстрого поиска объемов по ключам
            id_volume_dict = dict(zip(id_grouped['composite_key'], id_grouped['Объем_по_документу']))

            # Сопоставляем данные используя векторные операции
            self.log("Сопоставление данных...")
            self.update_progress(60, "Сопоставление данных")

            # Используем map для быстрого сопоставления
            structure_df['Подтверждено_ИД'] = structure_df['composite_key'].map(id_volume_dict).fillna(0)

            # Переносим результаты в исходный DataFrame
            pivot_df['Подтверждено_ИД'] = structure_df['Подтверждено_ИД']

            # Проверяем найденные соответствия
            matched_keys = structure_df[structure_df['Подтверждено_ИД'] > 0]['composite_key'].unique()

            self.update_progress(95, "Завершение сопоставления")

            # Статистика
            total_matched = (pivot_df['Подтверждено_ИД'] > 0).sum()
            total_volume = pivot_df['Подтверждено_ИД'].sum()

            self.log(f"✓ Найдено {total_matched} соответствий с ИД")
            self.log(f"✓ Общий объем подтвержденных работ: {total_volume}")

            if total_matched > 0:
                self.log("\nПримеры найденных соответствий:")
                matches = pivot_df[pivot_df['Подтверждено_ИД'] > 0].head(3)
                for _, match in matches.iterrows():
                    self.log(
                        f"  - Проект: '{match['Проект']}' | Конструкция: '{match['Конструкция']}' | Вид работ: '{match['Вид_работ_по_классификатору']}' -> Объем: {match['Подтверждено_ИД']}")
            else:
                self.log("\n✗ Совпадений не найдено. Возможные причины:")
                self.log("  1. Разные форматы данных в ИД и структуре")
                self.log("  2. Несовпадение значений в ключевых полях")
                self.log("  3. Проблемы с кодировкой или специальными символами")

                # Дополнительная диагностика
                common_keys = set(structure_df['composite_key']).intersection(set(id_df_work['composite_key']))
                self.log(f"  4. Пересечение ключей: {len(common_keys)} общих ключей")

                if len(common_keys) > 0:
                    self.log("  Примеры общих ключей:")
                    for key in list(common_keys)[:3]:
                        self.log(f"    - {key}")

            # Удаляем временные колонки
            if 'composite_key' in structure_df.columns:
                structure_df.drop('composite_key', axis=1, inplace=True)

            return pivot_df

        except Exception as e:
            self.log(f"✗ Ошибка при объединении данных: {str(e)}")
            import traceback
            traceback.print_exc()
            return pivot_df

    def create_final_report(self, pivot_df):
        """Создание финального отчета"""
        self.check_cancelled()
        self.log("Формирование финального отчета...")

        try:
            final_df = pivot_df.rename(columns={
                'Вид_работ_по_классификатору': 'Вид работ по классификатору',
                'Ед_изм': 'Ед. изм.',
                'Проектный_объем': 'Проектный объем',
                'Выполнено_в_натуре': 'Выполнено в натуре',
                'Принято_по_RFI': 'Принято по RFI (технадзор)',
                'Подтверждено_ИД': 'Подтверждено ИД'
            })

            column_order = [
                'Проект', 'Марка', 'Конструкция', 'Вид работ по классификатору',
                'Ед. изм.', 'Проектный объем', 'Выполнено в натуре',
                'Принято по RFI (технадзор)', 'Подтверждено ИД'
            ]

            existing_columns = [col for col in column_order if col in final_df.columns]
            final_df = final_df[existing_columns]

            return final_df

        except Exception as e:
            self.log(f"✗ Ошибка при создании финального отчета: {str(e)}")
            return pivot_df

    def process_files(self):
        """Основная функция обработки файлов"""
        try:
            self.update_progress(0, "Выбор папки с файлами...")
            folder_path = self.select_folder()

            if not folder_path:
                self.log("Папка не выбрана")
                return

            self.update_progress(5, "Поиск файлов...")
            id_files, structure_files = self.find_files_by_pattern(folder_path)

            if not id_files:
                self.log("✗ Файлы исполнительной документации не найдены")
                return

            if not structure_files:
                self.log("✗ Файлы структуры объекта не найдены")
                return

            id_file = id_files[0]
            structure_file = structure_files[0]

            self.log(f"✓ Найден файл ИД: {id_file}")
            self.log(f"✓ Найден файл структуры: {structure_file}")

            # Загрузка данных
            self.update_progress(10, "Загрузка данных структуры...")
            structure_df = self.load_structure_data(folder_path, structure_file)
            if structure_df is None:
                return

            self.update_progress(30, "Загрузка данных ИД...")
            id_df = self.load_id_data(folder_path, id_file)
            if id_df is None:
                return

            # Создаем сводную таблицу
            self.update_progress(50, "Создание сводной таблицы...")
            pivot_df = self.create_pivot_table(structure_df)
            if pivot_df is None:
                return

            # Объединяем с данными ИД
            self.update_progress(60, "Объединение данных...")
            result_df = self.optimize_matching(pivot_df, id_df)

            # Создаем финальный отчет
            self.update_progress(95, "Создание финального отчета...")
            final_report = self.create_final_report(result_df)

            # Сохранение результата
            output_file = os.path.join(folder_path, 'Отчет.xlsx')

            try:
                final_report.to_excel(output_file, sheet_name='Лист1', index=False)
                self.log(f"✓ Отчет успешно сохранен: {output_file}")

                # Статистика
                total_matched = (final_report[
                                     'Подтверждено ИД'] > 0).sum() if 'Подтверждено ИД' in final_report.columns else 0
                self.log("\n" + "=" * 50)
                self.log("ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО!")
                self.log("=" * 50)
                self.log(f"Всего записей в отчете: {len(final_report):,}")
                self.log(f"Найдено соответствий с ИД: {total_matched:,}")
                if len(final_report) > 0:
                    self.log(f"Процент соответствий: {total_matched / len(final_report) * 100:.1f}%")
                self.log(f"Результирующий файл: {output_file}")

                self.update_progress(100, "Обработка завершена!")

            except Exception as e:
                self.log(f"✗ Ошибка при сохранении отчета: {str(e)}")

        except Exception as e:
            if "отменена" not in str(e):
                self.log(f"Критическая ошибка: {str(e)}")

    def start_processing(self):
        """Запуск обработки в отдельном потоке"""
        self.progress_window = ProgressWindow()
        thread = threading.Thread(target=self.process_files)
        thread.daemon = True
        thread.start()

        # Проверяем завершение потока
        def check_thread():
            if thread.is_alive():
                self.progress_window.root.after(100, check_thread)
            else:
                self.progress_window.close()
                self.progress_window = None

        self.progress_window.root.after(100, check_thread)
        self.progress_window.root.mainloop()


class MainApplication:
    """Главное окно приложения"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Программа для создания отчета по структуре и ИД")
        self.root.geometry("500x300")
        self.root.resizable(False, False)

        self.processor = DataProcessor()

        self.create_widgets()

    def create_widgets(self):
        """Создание виджетов главного окна"""
        # Заголовок
        title_label = ttk.Label(self.root, text="Программа для создания отчета по структуре и ИД",
                                font=('Arial', 14, 'bold'))
        title_label.pack(pady=20)

        # Описание
        desc_text = """
Программа предназначена для создания отчета на основе:
- Файла структуры объекта ('Структура объекта ... .xlsx')
- Файла исполнительной документации ('Исполнительная документация от ... .xlsx')

Программа выполнит:
1. Загрузку и обработку данных
2. Сопоставление данных по трем условиям
3. Создание итогового отчета
        """
        desc_label = ttk.Label(self.root, text=desc_text, justify=tk.LEFT)
        desc_label.pack(pady=10, padx=20)

        # Кнопка запуска
        start_button = ttk.Button(self.root, text="Начать обработку",
                                  command=self.processor.start_processing)
        start_button.pack(pady=20)

        # Кнопка выхода
        exit_button = ttk.Button(self.root, text="Выход", command=self.root.quit)
        exit_button.pack(pady=5)

    def run(self):
        """Запуск приложения"""
        self.root.mainloop()


if __name__ == "__main__":
    app = MainApplication()
    app.run()