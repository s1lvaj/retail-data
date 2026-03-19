import subprocess


def run(step):
    print(f"Running: {step}")
    subprocess.run(["python", step], check=True)


def main():
    run("src/ingest/ingest.py")
    run("src/silver/silver_transform.py")
    run("src/gold/build_gold.py")


if __name__ == "__main__":
    main()