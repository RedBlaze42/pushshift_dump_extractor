import sqlite3

class AuthorDb:

    def __init__(self, db_path = "authors.db"):
        self.file_path  = db_path
        self.db = sqlite3.connect(self.file_path)
        
        self.db.execute("PRAGMA synchronous = NORMAL")
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.execute("PRAGMA cache_size = -32000")
        self.db.execute("PRAGMA SQLITE_THREADSAFE = 0")

        self.db.execute("CREATE TABLE IF NOT EXISTS author_posts (author_id INTEGER, subreddit_id INTEGER, posts INTEGER, UNIQUE(author_id , subreddit_id));")
        self.db.execute("CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY AUTOINCREMENT, reddit_id TEXT UNIQUE, name TEXT);")

    def __len__(self):
        return len(self.get_authors())

    def close(self):
        self.db.commit()
        self.db.close()

    def add_post(self, reddit_id, author_name, subreddit):
        author_id = self.get_author_id(reddit_id, author_name)
        self.db.execute("INSERT INTO author_posts (author_id, subreddit_id, posts) VALUES (?, ?, 1) ON CONFLICT(author_id, subreddit_id ) DO UPDATE SET posts = posts + 1;", (author_id, subreddit))
    
    def get_author_id(self, reddit_id, author_name):
        cursor = self.db.cursor()
        cursor.execute("SELECT id FROM authors WHERE reddit_id = ?", [reddit_id])
        author_id = cursor.fetchone()
        if author_id is None:
            cursor.execute("INSERT INTO authors (reddit_id, name) VALUES (?, ?)", (reddit_id, author_name))
            return cursor.lastrowid
        else:
            return author_id[0]

    def get_authors(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT DISTINCT author_id FROM author_posts;")
        author_ids = cursor.fetchall()
        authors = list()
        for author_id in author_ids:
            cursor.execute("SELECT name FROM authors WHERE id=?", [author_id[0]])
            authors.append(cursor.fetchone()[0])
        
        return authors
