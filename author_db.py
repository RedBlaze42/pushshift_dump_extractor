import sqlite3

class AuthorDb:

    def __init__(self, db_path = "authors.db"):
        self.file_path  = db_path
        self.db = sqlite3.connect(self.file_path)   

        self.db.execute("CREATE TABLE IF NOT EXISTS author_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_name TEXT, subreddit INTEGER, posts INTEGER);")

    def close(self):
        self.db.commit()
        self.db.close()

    def add_post(self, name, subreddit):
        self.db.execute("INSERT INTO author_posts (author_name, subreddit, posts) VALUES (?, ?, 1) ON DUPLICATE KEY UPDATE posts = posts + 1;", (name, subreddit))
    
    def get_authors(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT DISTINCT author_name FROM author_posts;")
        return cursor.fetchall()