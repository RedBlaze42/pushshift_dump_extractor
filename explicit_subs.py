from treat_dump_file import treat_file
import json

explicit_subs = list()

def treat_explicit_sub(sub):
    over_18_key = "over18" if "over18" in sub.keys() else "over_18"
    display_name_key = "display_name" if "display_name" in sub.keys() else "subreddit"
    if sub['subreddit_type'] == "public" and sub[over_18_key]:
        explicit_subs.append(sub[display_name_key])


if __name__ == '__main__':
    treat_file(treat_explicit_sub, "reddit_data/reddit_subreddits.ndjson.zst")

    with open("explicit_subs.json", "w") as f:
        json.dump(explicit_subs, f)