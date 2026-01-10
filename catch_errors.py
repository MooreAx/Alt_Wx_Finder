from pathlib import Path

TAF_DIR = Path("data")

def is_busted_taf_line(line: str) -> bool:
    line = line.strip()

    if " TEMPO=" in line or line.endswith("TEMPO="):
        return True

    return False


for file in TAF_DIR.glob("*.txt"):
    with file.open(encoding="utf-8") as f:
        busted_file = False

        for i, line in enumerate(f, start=1):
            if is_busted_taf_line(line):
                if not busted_file:
                    print(f"\n{file.name} -- problems found:")
                    busted_file = True

                print(f"  line {i}: {line.strip()}")


print("done")