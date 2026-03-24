import subprocess


def run(step):
    print(f"Running: {step}")
    subprocess.run(["python", step], check=True)


def main():
    run("src/ingest.py")
    run("src/silver_transform.py")
    run("src/build_gold.py")


if __name__ == "__main__":
    main()