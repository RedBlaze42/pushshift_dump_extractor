import sqlite3

class AuthorDb:

    def __init__(self, db_path = "authors.db"):
        self.file_path  = db_path
        self.db = sqlite3.connect(self.file_path)
        
        self.db.execute("PRAGMA synchronous = OFF")
        self.db.execute("PRAGMA journal_mode = OFF")

        self.db.execute("CREATE TABLE IF NOT EXISTS author_posts (author_name TEXT, subreddit INTEGER, posts INTEGER, UNIQUE(author_name, subreddit));")

    def close(self):
        self.db.commit()
        self.db.close()

    def add_post(self, name, subreddit):
        self.db.execute("INSERT INTO author_posts (author_name, subreddit, posts) VALUES (?, ?, 1) ON CONFLICT(author_name, subreddit) DO UPDATE SET posts = posts + 1;", (name, subreddit))
    
    def get_authors(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT DISTINCT author_name FROM author_posts;")
        return cursor.fetchall()