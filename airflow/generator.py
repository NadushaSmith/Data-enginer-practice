import os
import re
from jinja2 import Template
from datetime import datetime

SCRIPTS_DIR = "scripts"
TEMPLATES_DIR = "templates"
OUTPUT_DIR = "dags"


def extract_metadata(code, ext):
    """
    Извлекает метаданные из скрипта
    """
    metadata = {
        'schedule': '@daily',
        'description': '',
        'tasks': ''
    }

    lines = code.split('\n')

    if ext == '.py':
        for i, line in enumerate(lines):
            if line.strip().startswith('# METADATA:'):
                for j in range(i + 1, min(i + 10, len(lines))):
                    meta_line = lines[j].strip()
                    if meta_line.startswith('#'):
                        if ':' in meta_line:
                            key_value = meta_line[1:].strip().split(':', 1)
                            if len(key_value) == 2:
                                key = key_value[0].strip().lower()
                                value = key_value[1].strip()
                                metadata[key] = value
                    elif meta_line and not meta_line.startswith('#'):
                        break

    elif ext == '.sql':
        for i, line in enumerate(lines):
            if line.strip().startswith('-- METADATA:'):
                for j in range(i + 1, min(i + 10, len(lines))):
                    meta_line = lines[j].strip()
                    if meta_line.startswith('--'):
                        if ':' in meta_line:
                            key_value = meta_line[2:].strip().split(':', 1)
                            if len(key_value) == 2:
                                key = key_value[0].strip().lower()
                                value = key_value[1].strip()
                                metadata[key] = value
                    elif meta_line and not meta_line.startswith('--'):
                        break

    return metadata


def parse_python_functions(code):
    """Находит все функции в Python коде"""
    functions = []
    pattern = r'def\s+(\w+)\s*\(.*\)\s*:'
    matches = re.findall(pattern, code)
    return matches


def parse_sql_commands(code):
    """Разделяет SQL на отдельные команды"""
    commands = []
    # Убираем комментарии
    lines = code.split('\n')
    clean_lines = []

    for line in lines:
        if not line.strip().startswith('--'):
            clean_lines.append(line)

    clean_code = '\n'.join(clean_lines)

    # Разделяем по точкам с запятой
    raw_commands = clean_code.split(';')

    for cmd in raw_commands:
        cmd_clean = cmd.strip()
        if cmd_clean:
            first_word = cmd_clean.split()[0].upper() if cmd_clean.split() else ''
            cmd_type = 'QUERY'

            if first_word in ('CREATE', 'DROP', 'ALTER', 'TRUNCATE'):
                cmd_type = 'DDL'
            elif first_word in ('INSERT', 'UPDATE', 'DELETE'):
                cmd_type = 'DML'
            elif first_word == 'SELECT':
                cmd_type = 'SELECT'

            commands.append({
                'sql': cmd_clean + ';',
                'type': cmd_type
            })

    return commands


def load_template(template_path):
    """Загружает Jinja2 шаблон"""
    with open(template_path, encoding="utf-8") as f:
        return Template(f.read())


def parse_script_file(filepath):
    """Парсит один скрипт и возвращает структурированные данные"""
    with open(filepath, 'r', encoding='utf-8') as f:
        code = f.read()

    filename = os.path.basename(filepath)
    ext = os.path.splitext(filename)[1]

    metadata = extract_metadata(code, ext)

    if ext == '.py':
        functions = parse_python_functions(code)
        return {
            'filename': filename,
            'type': 'python',
            'code': code,
            'metadata': metadata,
            'functions': functions,
            'task_count': len(functions)
        }
    elif ext == '.sql':
        commands = parse_sql_commands(code)
        return {
            'filename': filename,
            'type': 'sql',
            'code': code,
            'metadata': metadata,
            'commands': commands,
            'task_count': len(commands)
        }
    return None


def generate_master_dag():
    """
    Создает ОДИН master DAG из всех скриптов в папке scripts/
    """
    print("=" * 50)
    print("🚀 Генерация MASTER DAG из всех скриптов")
    print("=" * 50)

    # Создаем папку для DAG'ов если её нет
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Собираем все скрипты
    all_scripts = []

    for fname in os.listdir(SCRIPTS_DIR):
        if not fname.endswith(('.py', '.sql')):
            continue

        filepath = os.path.join(SCRIPTS_DIR, fname)
        print(f"📄 Обработка: {fname}")

        try:
            script_data = parse_script_file(filepath)
            if script_data:
                all_scripts.append(script_data)
                print(f"   ✅ Найдено задач: {script_data['task_count']}")
        except Exception as e:
            print(f"   ❌ Ошибка: {str(e)}")

    if not all_scripts:
        print("⚠️  Нет скриптов для обработки")
        return

    # Разделяем скрипты по типам и находим конкретные файлы
    check_data_script = None
    monitor_script = None
    sql_script = None

    for script in all_scripts:
        filename = script['filename'].lower()
        if 'check' in filename and script['type'] == 'python':
            check_data_script = script
        elif 'monitor' in filename and script['type'] == 'python':
            monitor_script = script
        elif script['type'] == 'sql':
            sql_script = script

    # Подсчитываем задачи
    python_tasks_count = 0
    sql_tasks_count = 0

    if check_data_script:
        python_tasks_count += len(check_data_script.get('functions', []))
    if monitor_script:
        python_tasks_count += len(monitor_script.get('functions', []))
    if sql_script:
        sql_tasks_count += len(sql_script.get('commands', []))

    total_tasks = python_tasks_count + sql_tasks_count

    # Определяем общее расписание (берем из первого Python скрипта)
    common_schedule = '0 7 * * *'  # По умолчанию 07:00
    if check_data_script:
        common_schedule = check_data_script['metadata'].get('schedule', '0 7 * * *')

    # Определяем тег для DAG
    tag = "parallel"

    # Загружаем master шаблон
    template_path = os.path.join(TEMPLATES_DIR, "master_template.j2")
    template = load_template(template_path)

    # Рендерим DAG
    rendered = template.render(
        check_data_script=check_data_script,
        monitor_script=monitor_script,
        sql_script=sql_script,
        python_tasks_count=python_tasks_count,
        sql_tasks_count=sql_tasks_count,
        total_tasks=total_tasks,
        common_schedule=common_schedule,
        generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        tag=tag,
        dependencies=[]  # Пустой список зависимостей - все задачи параллельно
    )

    # Сохраняем ОДИН файл
    output_path = os.path.join(OUTPUT_DIR, "master_generated_dag.py")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered)

    print("\n" + "=" * 50)
    print("📊 СГЕНЕРИРОВАН ОДИН MASTER DAG:")
    print(f"   Скриптов обработано: {len(all_scripts)}")
    print(f"   Python задач: {python_tasks_count}")
    print(f"   SQL задач: {sql_tasks_count}")
    print(f"   Всего задач: {total_tasks}")
    print(f"   Расписание: {common_schedule}")
    print(f"   📁 Сохранен: {output_path}")
    print("=" * 50)

    # Выводим список созданных задач
    print("\n📋 СОЗДАННЫЕ ЗАДАЧИ:")

    if check_data_script:
        print(f"\n   Из {check_data_script['filename']}:")
        for func in check_data_script.get('functions', []):
            print(f"     • check_data_{func}")

    if monitor_script:
        print(f"\n   Из {monitor_script['filename']}:")
        for func in monitor_script.get('functions', []):
            print(f"     • monitor_{func}")

    if sql_script:
        print(f"\n   Из {sql_script['filename']}:")
        for i, cmd in enumerate(sql_script.get('commands', [])):
            print(f"     • validate_cmd_{i + 1}_{cmd['type'].lower()}")

    print("\n✅ Генерация завершена!")
    print("=" * 50)


if __name__ == "__main__":
    generate_master_dag()