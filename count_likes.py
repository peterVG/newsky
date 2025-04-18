# Import the Client class from the atproto library to interact with the Bluesky API.
from atproto import Client

# Import datetime, timezone, and timedelta for handling post timestamps and time windows.
from datetime import datetime, timezone, timedelta

# Import logging to output debug, info, and error messages for tracking script execution.
import logging

# Import load_dotenv to load environment variables from the .env file.
from dotenv import load_dotenv

# Import os to access environment variables (e.g., BLUESKY_HANDLE, BLUESKY_PASSWORD).
import os

# Import time to implement a timeout mechanism for the script.
import time

# Load environment variables from the .env file in the project directory.
# This makes BLUESKY_HANDLE and BLUESKY_PASSWORD available via os.getenv().
load_dotenv()

# Configure the logging system with DEBUG level to capture detailed execution logs.
# This helps diagnose issues like skipped posts or API errors.
logging.basicConfig(level=logging.DEBUG)

# Create a logger instance named after the main module for consistent log messages.
logger = logging.getLogger(__name__)

# Define the process_timeline function to fetch and process Bluesky posts.
# Parameters: time_window_hours (72 for 3 days), max_posts (100), top_n (5), timeout_seconds (300 for 5 minutes).
def process_timeline(time_window_hours=72, max_posts=100, top_n=5, timeout_seconds=300):
    """Fetch Bluesky posts and count likes, reposts, and replies from the timeline."""
    
    # Retrieve the Bluesky handle from the BLUESKY_HANDLE environment variable.
    handle = os.getenv('BLUESKY_HANDLE')
    
    # Retrieve the Bluesky password from the BLUESKY_PASSWORD environment variable.
    password = os.getenv('BLUESKY_PASSWORD')

    # Check if either handle or password is missing or empty.
    # If so, log an error and return empty lists to prevent further execution.
    if not handle or not password:
        logger.error("Missing BLUESKY_HANDLE or BLUESKY_PASSWORD in .env file")
        return [], [], []

    # Create a new Client instance to interact with the Bluesky API.
    client = Client()
    
    # Attempt to log in to Bluesky using the provided handle and password.
    # Wrap in try-except to handle authentication failures (e.g., invalid credentials).
    try:
        client.login(handle, password)
    except Exception as e:
        # Log the error with details if login fails and return empty lists.
        logger.error(f"Failed to login to Bluesky: {e}")
        return [], [], []

    # Initialize lists to store post data: one for likes, reposts, and replies.
    # Each post is stored as [text, count, uri] for sorting.
    post_likes = []
    post_reposts = []
    post_replies = []
    
    # Initialize a counter to track the number of processed posts.
    post_count = 0
    
    # Get the current UTC time as the reference point for the time window.
    start_time = datetime.now(timezone.utc)
    
    # Create a timedelta object for the time window (e.g., 72 hours).
    time_window = timedelta(hours=time_window_hours)
    
    # Initialize cursor as None for the first timeline fetch.
    # The cursor tracks pagination for subsequent API calls.
    cursor = None
    
    # Calculate the deadline for the script to stop (current time + timeout_seconds).
    deadline = time.time() + timeout_seconds

    # Loop until max_posts is reached or timeout occurs.
    # This handles pagination to fetch posts in batches.
    while post_count < max_posts and time.time() < deadline:
        # Calculate the number of posts to fetch in this batch (max 100 per API limit).
        # Ensures we don’t request more than max_posts - post_count.
        limit = min(100, max_posts - post_count)
        
        # Log the fetch attempt with the current limit and cursor for debugging.
        logger.debug(f"Fetching timeline with limit={limit}, cursor={cursor}")
        
        # Attempt to fetch a batch of posts from the timeline with the specified limit and cursor.
        # Wrap in try-except to handle API errors (e.g., rate limits, network issues).
        try:
            timeline = client.get_timeline(limit=limit, cursor=cursor)
        except Exception as e:
            # Log the error if the fetch fails and return empty lists.
            logger.error(f"Failed to fetch timeline: {e}")
            return [], [], []

        # Check if the timeline feed is empty (no more posts available).
        # If so, log and break the loop.
        if not timeline.feed:
            logger.info("No more posts in timeline")
            break

        # Iterate through each post in the timeline feed.
        for feed_view in timeline.feed:
            # Check if the timeout has been reached to prevent long runs.
            if time.time() >= deadline:
                # Log that the timeout was reached and break the loop.
                logger.info("Timeout reached, stopping")
                break

            # Extract the post object from the feed view.
            post = feed_view.post
            
            # Get the post’s creation timestamp from the record.
            created_at = post.record.created_at
            
            # Attempt to parse the timestamp into a datetime object.
            # Replace 'Z' with '+00:00' for ISO format compatibility.
            try:
                record_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except ValueError:
                # Log if the timestamp is invalid and skip the post.
                logger.debug(f"Skipping post with invalid time: {created_at}")
                continue
            
            # Check if the post is older than the time window (e.g., 72 hours).
            # If so, log and skip the post.
            if record_time < start_time - time_window:
                logger.debug(f"Skipping post outside time window: {created_at}")
                continue

            # Extract the post’s text content.
            post_text = post.record.text
            
            # Extract the post’s unique URI.
            post_uri = post.uri
            
            # Check if both text and URI are present (valid post).
            if post_text and post_uri:
                # Attempt to fetch the post’s thread to get like, repost, and reply counts.
                try:
                    post_thread = client.get_post_thread(post_uri)
                    # Get the like count, defaulting to 0 if missing.
                    like_count = post_thread.thread.post.like_count or 0
                    # Get the repost count, defaulting to 0 if missing.
                    repost_count = post_thread.thread.post.repost_count or 0
                    # Get the reply count, defaulting to 0 if missing.
                    reply_count = post_thread.thread.post.reply_count or 0
                except Exception as e:
                    # Log if fetching counts fails and set all to 0.
                    logger.warning(f"Failed to get counts for {post_uri}: {e}")
                    like_count = 0
                    repost_count = 0
                    reply_count = 0
                
                # Truncate the post text to 50 characters (with ellipsis if longer).
                # Append to post_likes, post_reposts, and post_replies as [text, count, uri].
                post_likes.append([post_text[:50] + "..." if len(post_text) > 50 else post_text, like_count, post_uri])
                post_reposts.append([post_text[:50] + "..." if len(post_text) > 50 else post_text, repost_count, post_uri])
                post_replies.append([post_text[:50] + "..." if len(post_text) > 50 else post_text, reply_count, post_uri])
                
                # Increment the post counter.
                post_count += 1
                
                # Log the processed post with its text, like, repost, and reply counts.
                logger.info(f"Processed post {post_count}: {post_text[:30]}... Likes: {like_count}, Reposts: {repost_count}, Replies: {reply_count}")

            # Check if max_posts has been reached to exit the loop.
            if post_count >= max_posts:
                break

        # Update the cursor with the next pagination token from the timeline response.
        cursor = timeline.cursor
        
        # If no cursor is provided, no more posts are available, so break the loop.
        if not cursor:
            logger.info("No more posts available (no cursor)")
            break

    # If no posts were processed, log this to indicate an empty result.
    if not post_likes:
        logger.info("No posts found within time window")

    # Sort posts by like count (index 1) in descending order and take top N.
    top_likes = sorted(post_likes, key=lambda x: x[1], reverse=True)[:top_n]
    
    # Sort posts by repost count (index 1) in descending order and take top N.
    top_reposts = sorted(post_reposts, key=lambda x: x[1], reverse=True)[:top_n]
    
    # Sort posts by reply count (index 1) in descending order and take top N.
    top_replies = sorted(post_replies, key=lambda x: x[1], reverse=True)[:top_n]
    
    # Return all three lists: top posts by likes, reposts, and replies.
    return top_likes, top_reposts, top_replies

# Define the main function to execute the script.
def main():
    # Call process_timeline to fetch and process posts, getting top likes, reposts, and replies.
    top_likes, top_reposts, top_replies = process_timeline()
    
    # Check if no posts were processed for likes.
    # If so, print a message.
    if not top_likes:
        print("No posts processed for likes.")
    
    # Print a header for the top-liked posts.
    print("\nTop 5 Most-Liked Posts on Bluesky (Last 72 Hours):")
    
    # Print a separator line for formatting.
    print("-" * 50)
    
    # Iterate through the top-liked posts, enumerating to get indices (1-based).
    for i, (text, like_count, uri) in enumerate(top_likes, 1):
        # Print each post’s details: index, like count, text, and URI.
        print(f"{i}. Likes: {like_count}\n   Text: {text}\n   URI: {uri}\n" + "-" * 50)

    # Check if no posts were processed for reposts.
    # If so, print a message.
    if not top_reposts:
        print("No posts processed for reposts.")
    
    # Print a header for the top-reposted posts.
    print("\nTop 5 Most-Reposted Posts on Bluesky (Last 72 Hours):")
    
    # Print a separator line for formatting.
    print("-" * 50)
    
    # Iterate through the top-reposted posts, enumerating to get indices (1-based).
    for i, (text, repost_count, uri) in enumerate(top_reposts, 1):
        # Print each post’s details: index, repost count, text, and URI.
        print(f"{i}. Reposts: {repost_count}\n   Text: {text}\n   URI: {uri}\n" + "-" * 50)

    # Check if no posts were processed for replies.
    # If so, print a message.
    if not top_replies:
        print("No posts processed for replies.")
    
    # Print a header for the top-replied posts.
    print("\nTop 5 Most-Replied Posts on Bluesky (Last 72 Hours):")
    
    # Print a separator line for formatting.
    print("-" * 50)
    
    # Iterate through the top-replied posts, enumerating to get indices (1-based).
    for i, (text, reply_count, uri) in enumerate(top_replies, 1):
        # Print each post’s details: index, reply count, text, and URI.
        print(f"{i}. Replies: {reply_count}\n   Text: {text}\n   URI: {uri}\n" + "-" * 50)

# Check if the script is being run directly (not imported as a module).
# If so, execute the main function.
if __name__ == "__main__":
    main()