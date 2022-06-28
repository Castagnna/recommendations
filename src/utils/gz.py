import gzip
import shutil


def gz_extract(input, output):

    try:
        with gzip.open(input, "rb") as f_in:
            with open(output, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        print("Extraction completed.")
    except Exception as e:
        print(f"Ops...Error: {e}")


def gz_compress(input, output):

    try:
        with open(input, "rb") as f_in:
            with gzip.open(output, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        print("Compression completed.")
    except Exception as e:
        print(f"Ops...Error: {e}")
