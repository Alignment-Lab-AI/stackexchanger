import argparse, traceback
from utils import *
from downloader import Stack_Exchange_Downloader
from pairer import QA_Pairer
import os
import py7zr

def print_logo():
    print("\033[1;34m")
    print("""
     _____  .__  .__                                        __     .____          ___.    
    /  _  \ |  | |__| ____   ____   _____   ____   ____   _/  |_   |    |   _____ \_ |__  
   /  /_\  \|  | |  |/ ___\ /    \ /     \_/ __ \ /    \  \   __\  |    |   \__  \ | __ \ 
  /    |    \  |_|  / /_/  >   |  \  Y Y  \  ___/|   |  \  |  |    |    |___ / __ \| \_\ \\
  \____|__  /____/__\___  /|___|  /__|_|  /\___  >___|  /  |__|    |_______ (____  /___  /
          \/       /_____/      \/      \/     \/     \/                   \/    \/    \/ 
    """)
    print("\033[0m")

def download_and_process_single(name, min_score, max_responses):
    try:
        name = name.strip().lower()
        dump_dir = os.path.join("dumps", name)
        os.makedirs(dump_dir, exist_ok=True)
        s = Stack_Exchange_Downloader(name)
        path_to_xml = os.path.join(dump_dir, "Posts.xml")
        path_to_7z = os.path.join("dumps", f"{name}.com.7z")
        out_folder = os.path.join("out", name)
        os.makedirs(out_folder, exist_ok=True)
        if not os.path.isfile(path_to_7z):
            # download 7z if it's not downloaded already
            s.download()
        if not os.path.isfile(path_to_xml):
            # extract 7z if it's not extracted already
            print(f"Extracting {path_to_7z} to {dump_dir}")
            with py7zr.SevenZipFile(path_to_7z, mode='r') as z:
                z.extractall(path=dump_dir)
        qa = QA_Pairer(path_to_xml, name=name, out_folder=out_folder, min_score=min_score, max_responses=max_responses)
        qa.process()
    except:
        traceback.print_exc()

def get_dump_names():
    s = Stack_Exchange_Downloader("all")
    dump_names = sorted([name for name in s.sites.keys() if ".meta." not in name])
    return dump_names

def select_dumps(dump_names):
    selected_dumps = []
    while True:
        print("Select a letter to view dumps (or type 'done' to finish):")
        letters = ["#"] + [chr(i) for i in range(65, 91)]  # Use uppercase letters
        for i in range(0, len(letters), 9):
            print(" ".join(f"\033[1;35m{letter:<2}\033[0m" for letter in letters[i:i+9]))
        print()
        
        choice = input().strip().lower()
        if choice == "done":
            break
        
        if choice in [l.lower() for l in letters]:
            print(f"Dumps starting with '{choice}':")
            dumps_to_show = [name for name in dump_names if name.startswith(choice)]
            for i in range(0, len(dumps_to_show), 4):
                for j in range(i, min(i+4, len(dumps_to_show))):
                    print(f"\033[1;34m{dumps_to_show[j].split('.')[0]:<20}\033[0m", end="")
                print()
            
            while True:
                print("Enter the name of the dump to process, or 'none' to go back:")
                selected = input().strip().lower()
                if selected == "none":
                    break
                elif any(selected == d.split('.')[0] for d in dumps_to_show):
                    full_name = next(d for d in dumps_to_show if d.split('.')[0] == selected)
                    selected_dumps.append(full_name)
                    dump_names = [d for d in dump_names if d != full_name]
                    break
                else:
                    print("Invalid choice. Please try again.")
        else:
            print("Invalid choice. Please try again.")
    
    return selected_dumps

def main():
    print_logo()
    dump_names = get_dump_names()
    selected_dumps = select_dumps(dump_names)
    
    if not selected_dumps:
        print("No dumps selected. Exiting.")
        return
    
    print('Downloading and processing stackexchange dumps for {}'.format(selected_dumps))
    # Download & Process
    for dump in selected_dumps:
        download_and_process_single(dump, args.min_score, args.max_responses)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='CLI for stackexchange_dataset - A tool for downloading & processing stackexchange dumps in xml form to a raw '
                    'question-answer pair text dataset for Language Models')
    parser.add_argument('--min_score', help='minimum score of a response in order to be included in the dataset. Default 3.',
                        type=int, default=0)
    parser.add_argument('--max_responses', help='maximum number of responses (sorted by score) to include for each question. '
                                                'Default 3.', type=int, default=None)
    args = parser.parse_args()
    main()

