from argparse import ArgumentParser
import csv
import sys
import re
from pathlib import Path

# CONFIG

OUTPUT_COLUMNS = [
    "id",
    "question",
    "model_text",
    "conditionId",
    "slug",
    "endDate",
    "closedTime"
]

REQUIRED_NON_NA = {"id", "closedTime"}

BANNED_KEYWORDS = {
    # General Events & Leagues
    "world cup", "championship", "champions", "super bowl", "olympics", "milano cortina",
    "nfl", "nba", "mlb", "nhl", "premier league", "serie a", "la liga", "bundesliga",
    "semifinal", "final", "playoff", "world series", "all star", "all-star",
    
    # F1 Teams & Key 2026 Drivers
    "formula 1", "f1", "grand prix", "red bull racing", "mercedes amg", "ferrari", 
    "mclaren", "aston martin", "audi f1", "cadillac f1", "williams racing", "alpine", 
    "haas", "racing bulls", "verstappen", "hamilton", "leclerc", "norris", "piastri", 
    "russell", "alonso", "sainz", "antonelli", "colapinto", "bearman", "hadjar", "perez",

# --- NBA (FULL 30 TEAMS & NICKNAMES) ---
    "hawks", "celtics", "nets", "hornets", "bulls", "cavaliers", "mavericks", "nuggets",
    "pistons", "warriors", "rockets", "pacers", "clippers", "lakers", "grizzlies", "heat",
    "bucks", "timberwolves", "pelicans", "knicks", "thunder", "magic", "76ers", "suns",
    "blazers", "kings", "spurs", "raptors", "jazz", "wizards",

    # --- NFL (FULL 32 TEAMS & NICKNAMES) ---
    "cardinals", "falcons", "ravens", "bills", "panthers", "bears", "bengals", "browns",
    "cowboys", "broncos", "lions", "packers", "texans", "colts", "jaguars", "chiefs",
    "raiders", "chargers", "rams", "dolphins", "vikings", "patriots", "saints", "giants",
    "jets", "eagles", "steelers", "49ers", "niners", "seahawks", "buccaneers", "titans", 
    "commanders",

    # --- MLB (FULL 30 TEAMS & NICKNAMES) ---
    "diamondbacks", "braves", "orioles", "red sox", "cubs", "white sox", "reds", "guardians",
    "rockies", "tigers", "astros", "royals", "angels", "dodgers", "marlins", "brewers",
    "twents", "mets", "yankees", "athletics", "phillies", "pirates", "padres", "giants",
    "mariners", "cardinals", "rays", "rangers", "blue jays", "nationals",

    # --- NHL (FULL 32 TEAMS & NICKNAMES) ---
    "ducks", "bruins", "sabres", "flames", "hurricanes", "blackhawks", "avalanche", 
    "blue jackets", "stars", "red wings", "oilers", "panthers", "kings", "wild", 
    "canadiens", "predators", "devils", "islanders", "rangers", "senators", "flyers", 
    "penguins", "sharks", "kraken", "blues", "lightning", "maple leafs", "canucks", 
    "golden knights", "capitals", "jets", "utah hockey club", "mammoth",

    # Crypto & Finance
    "crypto", "bitcoin", "ethereum", "solana", "dogecoin", "token", "etf",
    
    # Other
    "covid", "coronavirus", "weather", "musk", "tweet", "chess", "celcius", 

    # English Premier League
    "manchester city", "man city", "liverpool", "arsenal", "manchester united", "man utd", 
    "chelsea", "tottenham", "spurs", "newcastle", "aston villa", "everton", "west ham",

    # Spanish La Liga
    "real madrid", "barcelona", "barca", "atletico madrid", "atleti", "sevilla", 
    "villarreal", "real sociedad", "athletic club", "girona", "real betis",

    # German Bundesliga
    "bayern munich", "bayern munchen", "borussia dortmund", "bvb", "bayer leverkusen", 
    "rb leipzig", "eintracht frankfurt", "stuttgart", "wolfsburg", "hoffenheim",

    # Italian Serie A
    "inter milan", "ac milan", "juventus", "juve", "napoli", "as roma", "lazio", 
    "atalanta", "fiorentina", "bologna", "como",

    # French Ligue 1
    "psg", "paris saint-germain", "marseille", "lyon", "olympique lyonnais", "monaco", 
    "lille", "lens", "stade rennais", "nice",

    # Dutch Eredivisie
    "ajax", "psv eindhoven", "feyenoord", "az alkmaar", "twente", "utrecht", 
    "nec nijmegen", "groningen",

    # Crypto & Other (Retained from previous)
    "crypto", "bitcoin", "ethereum", "solana", "dogecoin", "token", "yield", "etf", "euro 2020",
    "covid", "coronavirus", "weather", "forecast", "elon musk", "tweet", "chess", "spread", 
    "ufc", "mma", 'boxing', "fight", "wwe", " lol", "dota", "valorant", "csgo", "S&P", "vs.", "derby"
}

REGEX_PATTERNS = [
    # Matchups: "Team vs Team", "Team @ Team", "Team v Team"
    re.compile(r"\b\w+\s*(vs|versus|@|v|/)\s*\w+\b", re.I),

    # \b ensures it doesn't match "ETHereum" or "BITCoin"
    re.compile(r"\b[A-Z]{3}\b"),

    # Major Tournaments
    re.compile(r"\b(ucl|uel|uefa|champions league|europa league|conference league)\b", re.I),

    # Scoreline patterns: "2-1", "0-0", "Win by 2+"
    re.compile(r"\d+\-\d+"),
    re.compile(r"win by \d\+?"),

    # Common Soccer Phrases
    re.compile(r"\b(clean sheet|both teams to score|btts|anytime goalscorer|hat-trick)\b", re.I),

    # Specific 2026 Suffixes
    re.compile(r"\b(fc|united|city|real|athletic|sporting|olympique|as|ac)\b", re.I),

    # Captures: "Will [Team/Player] win?" or "Will [Team/Player] draw?"
    # \b ensures we match whole words; .*? is a non-greedy match for the content in between
    re.compile(r"\bWill\b.*?\b(win|draw)\b\?", re.I),

    # Bonus: Captures "Who will win: [Team] or [Team]?"
    re.compile(r"\bWho\b.*?\bwin\b.*?\?", re.I),

    # \bWill\b.*?\b(say|mention)\b -> Matches "Will" then "say/mention"
    # \s*['"](.*?)['"] -> Matches the quoted part (non-greedy)
    re.compile(r"\bWill\b.*?\b(say|mention)\b\s*['\"](.*?)['\"]\b.*?\?", re.I),
]

SENTENCE_SPLIT_REGEX = re.compile(r"(?<=[.!?])\s+")


def is_na(val):
    if val is None:
        return True
    v = str(val).strip().lower()
    return v in {"", "na", "n/a", "nan", "none", "null"}


def first_sentence(text: str) -> str:
    """
    Extract the first sentence from description.
    Falls back gracefully if punctuation is missing.
    """
    if not isinstance(text, str):
        return ""

    text = text.strip()
    if not text:
        return ""

    parts = SENTENCE_SPLIT_REGEX.split(text, maxsplit=1)
    return parts[0].strip()


def question_is_banned(question: str) -> bool:
    if not isinstance(question, str):
        return True

    q = question.lower()

    for kw in BANNED_KEYWORDS:
        if kw in q:
            return True

    for rx in REGEX_PATTERNS:
        if rx.search(question):
            return True

    return False


# MAIN CLEANER

def clean_csv(input_path: Path, output_path: Path):
    counts = {
        "read": 0,
        "dropped_na": 0,
        "dropped_question_banned": 0,
        "dropped_duplicate_id": 0,
        "dropped_malformed": 0,
        "written": 0,
    }

    seen_ids = set()

    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as fin, \
         output_path.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin, skipinitialspace=True)
        reader.fieldnames = [f.strip() for f in reader.fieldnames]

        writer = csv.DictWriter(
            fout,
            fieldnames=OUTPUT_COLUMNS,
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()

        for row in reader:
            counts["read"] += 1

            # Malformed rows (JSON comma issues)
            if row.get(None):
                counts["dropped_malformed"] += 1
                continue

            # Required fields
            if any(is_na(row.get(col)) for col in REQUIRED_NON_NA):
                counts["dropped_na"] += 1
                continue

            question = row.get("question", "")
            description = row.get("description", "")

            # Question filter
            if question_is_banned(question):
                counts["dropped_question_banned"] += 1
                continue

            # Deduplicate ID
            id_key = str(row["id"]).strip()
            if id_key in seen_ids:
                counts["dropped_duplicate_id"] += 1
                continue
            seen_ids.add(id_key)

            desc_first = first_sentence(description)

            if desc_first:
                model_text = f"{question}. {desc_first}"
            else:
                model_text = question

            row["model_text"] = model_text

            writer.writerow(row)
            counts["written"] += 1

    print("\n✅ CSV cleaning complete\n")
    for k, v in counts.items():
        print(f"{k:28}: {v}")


# CLI

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print("Input file not found", file=sys.stderr)
        sys.exit(2)

    clean_csv(input_path, output_path)
