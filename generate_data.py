# Databricks notebook source
# Step 1: generate fake but realistic betting data for two days (day1 and day2).
# It writes CSV files into the landing volume, like a betting system dropping its daily exports.

# COMMAND ----------
# Settings
import csv
import os
import random
from datetime import datetime, timedelta

# Fixed seed: every run creates exactly the same data
random.seed(42)

LANDING = "/Volumes/workspace/betting/landing"
DAY1 = datetime(2026, 10, 26)
DAY2 = datetime(2026, 10, 27)

# COMMAND ----------
# Helpers

# Write a list of rows into a CSV file with a header line
def write_csv(folder, file_name, header, rows):
    os.makedirs(folder, exist_ok=True)
    path = folder + "/" + file_name
    with open(path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    print("wrote", len(rows), "rows to", path)

# A random time inside one day, as text
def random_time(day):
    seconds = random.randint(0, 86399)
    moment = day + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%d %H:%M:%S")

# Pick a random value from the options that is different from the current one
def different_value(options, current):
    choices = []
    for option in options:
        if option != current:
            choices.append(option)
    return random.choice(choices)

# COMMAND ----------
# Partners: the 3 B2B betting operators that use the platform
partner_header = ["partner_id", "partner_name", "country"]
partners = [
    ["P-1", "AlpenBet", "AT"],
    ["P-2", "NordWette", "DE"],
    ["P-3", "AdriaSport", "HR"],
]

# COMMAND ----------
# Customers: 300, each belongs to one partner
customer_header = ["customer_id", "partner_id", "country", "risk_segment"]
countries = ["AT", "DE", "HR", "IT"]
segments = ["low", "medium", "high"]

customers_day1 = []
for number in range(1, 301):
    customer_id = "C-" + str(number).zfill(3)
    partner = random.choice(partners)
    partner_id = partner[0]
    country = random.choice(countries)
    segment = random.choice(segments)
    customers_day1.append([customer_id, partner_id, country, segment])

# Day 2: the same customers, but 10 of them change country or risk segment (needed for SCD2 later)
customers_day2 = []
for customer in customers_day1:
    copy = list(customer)  # a copy, so the day 1 list stays unchanged
    customers_day2.append(copy)

changed_customers = random.sample(customers_day2, 10)
for customer in changed_customers:
    if random.random() < 0.5:
        customer[2] = different_value(countries, customer[2])  # new country
    else:
        customer[3] = different_value(segments, customer[3])   # new risk segment

# COMMAND ----------
# Events: 40 matches (28 football, 12 tennis), half on day 1 and half on day 2
event_header = ["event_id", "sport", "home_team", "away_team", "start_time"]
football_teams = ["Rapid Wien", "Austria Wien", "Sturm Graz", "Red Bull Salzburg", "LASK", "Wolfsberger AC"]
tennis_players = ["Rune", "Alcaraz", "Sinner", "Djokovic", "Zverev", "Medvedev"]

events = []
for number in range(1, 41):
    event_id = "E-" + str(number).zfill(3)
    if number <= 28:
        sport = "football"
        teams = random.sample(football_teams, 2)
    else:
        sport = "tennis"
        teams = random.sample(tennis_players, 2)
    if number % 2 == 1:
        day = DAY1
    else:
        day = DAY2
    start_time = random_time(day)
    events.append([event_id, sport, teams[0], teams[1], start_time])

# COMMAND ----------
# Bets: helpers to create one bet
bet_header = ["bet_id", "customer_id", "partner_id", "event_id", "market", "selection",
              "stake", "odds", "status", "placed_at", "updated_at"]
stakes = [2, 5, 10, 20, 50, 100]

# Decide the result of a bet.
# The chance to win is a bit lower than 1 / odds (0.93 instead of 1). That gap is the bookmaker's margin (about 7%).
def settle(odds):
    if random.random() < 0.02:
        return "void"
    chance_to_win = 0.93 / odds
    if random.random() < chance_to_win:
        return "won"
    return "lost"

# Create one bet. settled_share = the share of bets that are already settled when the file is exported
def make_bet(bet_number, day, settled_share):
    bet_id = "B-" + str(bet_number).zfill(6)
    customer = random.choice(customers_day1)
    customer_id = customer[0]
    partner_id = customer[1]
    event = random.choice(events)
    event_id = event[0]
    market = "match_winner"
    if event[1] == "football":
        selection = random.choice(["home", "draw", "away"])
    else:
        selection = random.choice(["home", "away"])
    stake = random.choice(stakes)
    odds = round(random.uniform(1.25, 3.5), 2)  # most real bets are on short odds
    placed_at = random_time(day)
    if random.random() < settled_share:
        status = settle(odds)
    else:
        status = "open"
    updated_at = placed_at  # first version of the bet
    return [bet_id, customer_id, partner_id, event_id, market, selection,
            stake, odds, status, placed_at, updated_at]

# COMMAND ----------
# Day 1: 5,000 bets, 60% already settled, 40% still open
bets_day1 = []
for number in range(1, 5001):
    bet = make_bet(number, DAY1, 0.6)
    bets_day1.append(bet)

# Day 2, part a: 2,000 new bets
bets_day2 = []
for number in range(5001, 7001):
    bet = make_bet(number, DAY2, 0.7)
    bets_day2.append(bet)

# Day 2, part b: settlement updates. Every bet that was open on day 1 is now settled,
# with a newer updated_at. Silver must keep this newer version.
settlement_updates = 0
for bet in bets_day1:
    if bet[8] == "open":
        update = list(bet)
        update[8] = settle(bet[7])
        update[10] = random_time(DAY2)
        bets_day2.append(update)
        settlement_updates = settlement_updates + 1
print("settlement updates in day 2:", settlement_updates)

# COMMAND ----------
# Built-in problems (on purpose): break 100 bets per day, each in one way. Silver must catch them.
def add_errors(bets):
    broken_bets = random.sample(bets, 100)
    for position in range(100):
        bet = broken_bets[position]
        if position < 30:
            bet[1] = ""                       # missing customer_id
        elif position < 50:
            bet[6] = random.choice([0, -5])   # stake 0 or negative
        elif position < 70:
            bet[7] = 1.0                      # odds below 1.01
        elif position < 90:
            bet[3] = "E-999"                  # event that does not exist
        else:
            bet[9] = "2026-13-45 25:61:00"    # broken timestamp

add_errors(bets_day1)
add_errors(bets_day2)

# Day 2, part c: 200 exact duplicates (the source re-delivered some rows)
duplicates = random.sample(bets_day2, 200)
for bet in duplicates:
    bets_day2.append(list(bet))

# Mix the order, like a real export
random.shuffle(bets_day2)

# COMMAND ----------
# Write all files into the landing volume
folder_day1 = LANDING + "/day1"
write_csv(folder_day1, "partners.csv", partner_header, partners)
write_csv(folder_day1, "customers.csv", customer_header, customers_day1)
write_csv(folder_day1, "events.csv", event_header, events)
write_csv(folder_day1, "bets.csv", bet_header, bets_day1)

folder_day2 = LANDING + "/day2"
write_csv(folder_day2, "partners.csv", partner_header, partners)
write_csv(folder_day2, "customers.csv", customer_header, customers_day2)
write_csv(folder_day2, "events.csv", event_header, events)
write_csv(folder_day2, "bets.csv", bet_header, bets_day2)

# COMMAND ----------
# Check: look at the day 2 bets file with Spark
day2_bets = spark.read.option("header", True).csv(LANDING + "/day2/bets.csv")
print("rows in day2/bets.csv:", day2_bets.count())
display(day2_bets.limit(10))
