from atproto import Client
from datetime import datetime, timezone, timedelta
import logging
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def process_timeline(time_window_hours=72, max_posts=100, top_n=5, timeout_seconds=300):
    """Fetch Bluesky posts and likes from the timeline."""
    # Get credentials from environment variables
    handle = os.getenv('BLUESKY_HANDLE')
    password = os.getenv('BLUESKY_PASSWORD')

    if not handle or not password:
        logger.error("Missing BLUESKY_HANDLE or BLUESKY_PASSWORD in .env file")
        return []

    client = Client()
    try:
        client.login(handle, password)
    except Exception as e:
        logger.error(f"Failed to login to Bluesky: {e}")
        return []

    post_likes = []
    post_count = 0
    start_time = datetime.now(timezone.utc)
    time_window = timedelta(hours=time_window_hours)
    cursor = None
    deadline = time.time() + timeout_seconds

    # Fetch timeline in batches of 100 posts
    while post_count < max_posts and time.time() < deadline:
        try:
            limit = min(100, max_posts - post_count)  # Respect API limit of 100
            logger.debug(f"Fetching timeline with limit={limit}, cursor={cursor}")
            timeline = client.get_timeline(limit=limit, cursor=cursor)
        except Exception as e:
            logger.error(f"Failed to fetch timeline: {e}")
            return []

        if not timeline.feed:
            logger.info("No more posts in timeline")
            break

        for feed_view in timeline.feed:
            if time.time() >= deadline:
                logger.info("Timeout reached, stopping")
                break

            post = feed_view.post
            created_at = post.record.created_at
            try:
                record_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except ValueError:
                logger.debug(f"Skipping post with invalid time: {created_at}")
                continue
            if record_time < start_time - time_window:
                logger.debug(f"Skipping post outside time window: {created_at}")
                continue

            post_text = post.record.text
            post_uri = post.uri
            if post_text and post_uri:
                # Fetch like count
                try:
                    post_thread = client.get_post_thread(post_uri)
                    like_count = post_thread.thread.post.like_count or 0
                except Exception as e:
                    logger.warning(f"Failed to get like count for {post_uri}: {e}")
                    like_count = 0
                post_likes.append([post_text[:50] + "..." if len(post_text) > 50 else post_text, like_count, post_uri])
                post_count += 1
                logger.info(f"Processed post {post_count}: {post_text[:30]}... Likes: {like_count}")

            if post_count >= max_posts:
                break

        # Update cursor for next batch
        cursor = timeline.cursor
        if not cursor:
            logger.info("No more posts available (no cursor)")
            break

    if not post_likes:
        logger.info("No posts found within time window")

    return sorted(post_likes, key=lambda x: x[1], reverse=True)[:top_n]

def main():
    top_likes = process_timeline()
    if not top_likes:
        print("No posts processed.")
        return

    print("\nTop 5 Most-Liked Posts on Bluesky (Last 24 Hours):")
    print("-" * 50)
    for i, (text, like_count, uri) in enumerate(top_likes, 1):
        print(f"{i}. Likes: {like_count}\n   Text: {text}\n   URI: {uri}\n" + "-" * 50)

if __name__ == "__main__":
    main()