from treat_dump_file import treat_file, treat_files
import json, ndjson, os
import author_db

if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)
    max_sub_id = 0
    subs_ids = dict()
    subs = dict()
    authors = author_db.AuthorDb("output/authors.db")

def treat_comment(comment):
    global max_sub_id, subs_ids, subs, authors
    if comment["subreddit"] not in subs_ids.keys():
        sub_id = max_sub_id + 1
        subs_ids[comment["subreddit"]] = sub_id
        max_sub_id += 1
    else:
        sub_id = subs_ids[comment["subreddit"]]

    if sub_id not in subs.keys(): subs[sub_id] = 1
    subs[sub_id] += 1
    
    authors.add_post(comment["author"], sub_id)

if __name__ == "__main__":
    
    file_name = "reddit_data/RC*.zst"
    try:
        treat_files(treat_comment, file_name)
    except KeyboardInterrupt:
        print("Arrêt...")

    authors.close()

    with open("output/subreddits.json","w") as f:
        json.dump(subs, f)

    with open("output/subreddits_ids.json","w") as f:
        json.dump(subs_ids, f)

    with open("output/dump_infos.json","w") as f:
        json.dump({"element_number": len(authors)}, f)