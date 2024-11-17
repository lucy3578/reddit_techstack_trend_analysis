import praw

class SubRedditDataAPI:
    def __init__(self):
        self.client_id = ""
        self.client_secret = ""
        self.user_agent = "MyRedditScraper/1.0 by ___"
        self.username = ""
        self.password = ""

        # Authenticate with PRAW
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
            username=self.username,
            password=self.password
        )

    def get_reddit_request(self, subreddit: str):
        # Access the subreddit
        subreddit = self.reddit.subreddit(subreddit)
        return subreddit



