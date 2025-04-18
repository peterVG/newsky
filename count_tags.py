# Import necessary classes and functions from the atproto library for Firehose access and API queries
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR, Client
# Import Counter for counting hashtag occurrences
from collections import Counter
# Import time for handling timing of print updates
import time

# Create an instance of the Client for making API queries, such as fetching user profiles
at_client = Client()

# Initialize a Counter to keep track of hashtag frequencies
hashtag_counts = Counter()
# Record the last time the top hashtags were printed
last_print_time = time.time()
# Set the interval for printing updates (10 seconds)
print_interval = 10  # seconds

# Define a function to handle messages received from the Firehose
def on_message_handler(message):
    # Declare global variables to modify them inside the function
    global last_print_time, hashtag_counts
    
    # Parse the incoming message to extract the commit object
    commit = parse_subscribe_repos_message(message)
    
    # Check if the commit is of the expected type (ComAtprotoSyncSubscribeRepos.Commit)
    # If not, exit early as this message is not relevant
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    
    # Check if the commit contains any blocks (data chunks)
    # If not, exit early as there's no data to process
    if not commit.blocks:
        return
    
    # Create a CAR object from the commit's blocks
    # CAR is used to store and retrieve data in a decentralized manner
    car = CAR.from_bytes(commit.blocks)
    
    # Extract the author's DID (Decentralized Identifier) from the commit
    author_did = commit.repo
    
    # Attempt to retrieve the author's profile using their DID
    # This query fetches the user's handle (e.g., username) from the BlueSky API
    try:
        profile = at_client.query('com.atproto.identity.getProfile', data={'did': author_did})
        author_handle = profile['handle']
    except:
        # If the query fails (e.g., due to network issues or invalid DID), use the DID as the handle
        author_handle = author_did
    
    # Iterate over each operation in the commit
    # Operations represent changes like creating, updating, or deleting records
    for op in commit.ops:
        # Check if the operation is a "create" action and has a CID (Content Identifier)
        # We're only interested in new posts being created
        if op.action in ["create"] and op.cid:
            # Retrieve the data associated with the CID from the CAR object
            data = car.blocks.get(op.cid)
            
            # Check if the data exists and is of type 'app.bsky.feed.post' (a BlueSky post)
            if data and data['$type'] == 'app.bsky.feed.post':
                # Extract the text content of the post
                text = data.get('text', '')
                
                # Split the post text into individual words
                words = text.split()
                
                # Extract hashtags by filtering words that start with '#' and convert to lowercase
                # Lowercase conversion ensures case-insensitive counting (e.g., #Tech and #tech are treated the same)
                hashtags = [word.lower() for word in words if word.startswith('#')]
                
                # Update the hashtag counts with the new hashtags
                hashtag_counts.update(hashtags)
    
    # Get the current time
    current_time = time.time()
    
    # Check if 10 seconds have passed since the last print
    if current_time - last_print_time >= print_interval:
        # If yes, print the top 20 hashtags
        print_top_hashtags()
        # Update the last print time
        last_print_time = current_time

# Function to print the top 20 hashtags with their counts
def print_top_hashtags():
    # Print a header with the current time
    print(f"Top 20 hashtags at {time.ctime()}:")
    # Get the top 20 most common hashtags
    for hashtag, count in hashtag_counts.most_common(20):
        # Print each hashtag and its count
        print(f"{hashtag}: {count}")
    # Print a separator line for clarity between updates
    print("-" * 40)

# Create an instance of FirehoseSubscribeReposClient to connect to the BlueSky Firehose
firehose_client = FirehoseSubscribeReposClient()

# Start the Firehose client and begin processing messages using the on_message_handler function
firehose_client.start(on_message_handler)