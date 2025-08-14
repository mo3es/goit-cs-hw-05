import asyncio
import aiofiles
import aiofiles.os as aos
import logging
from pathlib import Path
import argparse
import os
from typing import Set


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("sorting.log"), logging.StreamHandler()],
)


async def read_folder(source_folder: Path, output_folder: Path) -> Set[asyncio.Task]:
    tasks = set()
    try:
        loop = asyncio.get_running_loop()
        entries = await loop.run_in_executor(None, os.scandir, source_folder)

        for entry in entries:
            path = Path(entry.path)
            if entry.is_dir():
                tasks.update(await read_folder(path, output_folder))
            elif entry.is_file():
                if path.name != "sorting.log":
                    task = asyncio.create_task(copy_file(path, output_folder))
                    tasks.add(task)
    except OSError as e:
        logging.error(f"Error directory access - '{source_folder}': {e}")
    return tasks


async def copy_file(file_path: Path, output_folder: Path):
    try:
        extension = file_path.suffix.lstrip(".").lower() or "other"
        destination_folder = output_folder / extension
        await aos.makedirs(destination_folder, exist_ok=True)

        destination_path = destination_folder / file_path.name
        async with aiofiles.open(file_path, "rb") as f_in:
            async with aiofiles.open(destination_path, "wb") as f_out:
                while True:
                    chunk = await f_in.read(8192)
                    if not chunk:
                        break
                    await f_out.write(chunk)

        logging.info(f"File '{file_path}' copied to '{destination_path}'")
    except Exception as e:
        logging.error(f"Error file copying '{file_path}': {e}")


async def main(source_folder: Path, output_folder: Path):
    logging.info(f"Start sorting from '{source_folder}' to '{output_folder}'")

    tasks = await read_folder(source_folder, output_folder)

    if not tasks:
        logging.info("Files not found.")
        return

    await asyncio.gather(*tasks)

    logging.info("All tasks completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async file copying (by type).")
    parser.add_argument("source_folder", type=Path, help="Path to source folder.")
    parser.add_argument("output_folder", type=Path, help="Path to destination folder.")

    args = parser.parse_args()

    if not args.source_folder.is_dir():
        print(f"Error: source folder '{args.source_folder}' is not exist.")
        exit(1)

    if args.source_folder == args.output_folder:
        print("Error: source and destination paths is similar.")
        exit(1)

    if args.output_folder.is_dir():
        try:
            import shutil

            shutil.rmtree(args.output_folder)
            print("Destination folder was exist and was deleted.")
        except Exception as e:
            print(f"Error while clearing folder: {e}")
            exit(1)

    try:
        asyncio.run(main(args.source_folder, args.output_folder))
    except KeyboardInterrupt:
        logging.warning("Sorting was interrupted by keyboard input.")
    except Exception as e:
        logging.error(f"Error: {e}")
