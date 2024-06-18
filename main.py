import os
import re
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

# Путь к директории с логами
logs_path = "logs"

log_pattern = re.compile(
    r"\[(\d{2}:\d{2}:\d{2})\]\s\[(?P<notygroup>.+)\]\s\[(?P<package>.+)]: (?P<message>.+)"
)
connect_pattern = re.compile(r"UUID of player (?P<player>\w+) is \S+")
disconnect_or_kick_pattern = re.compile(
    r"(?P<player>\w+) lost connection: (?P<reason>.+)|"
    r"Disconnecting com.mojang.authlib.GameProfile@[\da-f-]+,name=(?P<player_kick>\w+),.+: (?P<reason_kick>.+)"
)

# Словари для хранения данных
playtime = {}
first_login = {}
last_login = {}
login_times = {}
retention = {}
retention_days = set()


def parse_time(time_str):
    return datetime.strptime(time_str, "%H:%M:%S")


def update_playtime(player, login_time, logout_time=None):
    if player not in playtime:
        playtime[player] = timedelta()
        #print(f"playtime[{player}] = {timedelta()}")
    if logout_time:
        #print(f"{playtime[player]} + ({logout_time} - {login_time}) = {playtime[player] + (logout_time - login_time)}")
        playtime[player] += (logout_time - login_time)
    else:
        #print(f"{playtime[player]} + ({datetime.now()} - {login_time}) = {playtime[player] + (datetime.now() - login_time)}")
        playtime[player] += (datetime.now() - login_time)


with tqdm(total=len(os.listdir(logs_path)), desc='Чтение логов', unit='file') as pbar:
    for log_filename in sorted(os.listdir(logs_path)):
        if not log_filename.endswith(".log"):
            pbar.update(1)
            continue

        date_parts = log_filename.split('-')[0:3]
        if date_parts:
            log_date = datetime.strptime("-".join(date_parts), "%Y-%m-%d")

        with open(os.path.join(logs_path, log_filename), "r", encoding="utf-8") as log_file:
            for line in log_file:
                match = log_pattern.match(line)
                if not match:
                    continue

                time_str, _, _, message = match.groups()
                time_part = parse_time(time_str)

                # Объединяем дату и время для создания полного datetime
                log_datetime = datetime.combine(log_date, time_part.time())

                connect_match = connect_pattern.search(message)
                if connect_match:
                    player = connect_match.group("player")
                    player = player.lower()

                    # if player != "iwishna":
                    #     continue
                    # print(f"{log_datetime} - {message}")

                    # Устанавливаем время входа в игру
                    if (player not in login_times) or ('logout_time' in login_times[player]):
                        login_times[player] = {"login_time": log_datetime}
                        if player not in first_login:
                            first_login[player] = log_datetime
                            retention_days.add(first_login[player].date())

                disconnect_match = disconnect_or_kick_pattern.search(message)
                if disconnect_match:
                    player = disconnect_match.group("player") or disconnect_match.group("player_kick")
                    player = player.lower()

                    # if player != "iwishna":
                    #     continue
                    # print(f"{log_datetime} - {message}")

                    if player in login_times:
                        login_time = login_times[player].get("login_time")
                        if login_time:
                            logout_time = log_datetime
                            update_playtime(player, login_time, logout_time)
                            login_times[player]["logout_time"] = logout_time
                            last_login[player] = logout_time

        pbar.update(1)

#quit();

# Обрабатываем оставшиеся случаи, если игроки не вышли
for player, timestamps in login_times.items():
    if "login_time" in timestamps and "logout_time" not in timestamps:
        player = player.lower()
        update_playtime(player, timestamps["login_time"])

# Подсчёт ретенции игроков
retention_counts = {
    "date": list(retention_days),
    "new_players": [1] * len(retention_days)
}
retention_df = pd.DataFrame(retention_counts)

# Подсчёт времени игры
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