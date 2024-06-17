import os
import re
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

# Путь к директории с логами
logs_path = "logs"  # замените на путь к вашим логам

log_pattern = re.compile(
    r"\[(\d{2}:\d{2}:\d{2})\]\s\[(?P<notygroup>.+)\]\s\[(?P<package>.+)]: (?P<message>.+)"
)
# connect_pattern = re.compile(r"(?P<player>\w+) joined the game")
connect_pattern = re.compile(r"UUID of player (?P<player>\w+)")
disconnect_pattern = re.compile(r"(?P<player>\w+) lost connection: (?P<reason>.+)")

# Словари для хранения данных
playtime = {}
first_login = {}
last_login = {}
login_times = {}
retention = {}


def parse_time(time_str):
    return datetime.strptime(time_str, "%H:%M:%S")


def update_playtime(player, login_time, logout_time=None):
    if player not in playtime:
        playtime[player] = timedelta()
    if logout_time:
        playtime[player] += logout_time - login_time
    else:
        playtime[player] += datetime.now() - login_time

with tqdm(total=len(os.listdir(logs_path)), desc='Чтение логов', unit='file') as pbar:
    for log_filename in sorted(os.listdir(logs_path)):
        if not log_filename.endswith(".log"):
            pbar.update(1)
            continue

        date_str = log_filename.split('-')[0:3]
        if date_str:
            log_date = datetime.strptime("-".join(date_str), "%Y-%m-%d")

        with open(os.path.join(logs_path, log_filename), "r", encoding="utf-8") as log_file:  # Добавили encoding="utf-8"
            for line in log_file:
                match = log_pattern.match(line)
                if not match:
                    continue

                time_str, notygroup, package, message = match.groups()
                time = parse_time(time_str)

                connect_match = connect_pattern.search(message)
                if connect_match:
                    player = connect_match.group("player")
                    if player not in login_times or login_times[player].get("logout_time"):
                        if player not in first_login:
                            first_login[player] = log_date
                            retention[log_date] = retention.get(log_date, 0) + 1
                        login_times[player] = {
                            "login_time": log_date + timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)}

                disconnect_match = disconnect_pattern.search(message)
                if disconnect_match:
                    player = disconnect_match.group("player")
                    if player in login_times and "login_time" in login_times[player]:
                        login_time = login_times[player]["login_time"]
                        logout_time = log_date + timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)
                        update_playtime(player, login_time, logout_time)
                        login_times[player]["logout_time"] = logout_time

        pbar.update(1)

# Обрабатываем оставшиеся если игрок не выходил
for player, timestamps in login_times.items():
    if "login_time" in timestamps and "logout_time" not in timestamps:
        update_playtime(player, timestamps["login_time"])

retention_counts = {"date": list(retention.keys()), "new_players": list(retention.values())}
retention_df = pd.DataFrame(retention_counts)

playtime_data = {"player": [], "playtime_hours": []}
for player, total_playtime in playtime.items():
    playtime_data["player"].append(player)
    playtime_data["playtime_hours"].append(total_playtime.total_seconds() / 3600)

playtime_df = pd.DataFrame(playtime_data)

retention_data = {"player": [], "retention": []}
for player in playtime_data["player"]:
    retention_data["player"].append(player)
    retention_data["retention"].append("True" if first_login[player] != last_login.get(player) else "False")

retention_df2 = pd.DataFrame(retention_data)

# Экспорт в Excel
with pd.ExcelWriter('minecraft_logs.xlsx') as writer:
    playtime_df.to_excel(writer, sheet_name='Playtime', index=False)
    retention_df.to_excel(writer, sheet_name='Retention', index=False)
    retention_df2.to_excel(writer, sheet_name='Player Retention', index=False)

print("Данные успешно экспортированы в 'minecraft_logs.xlsx'")