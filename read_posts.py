# Import necessary modules from the atproto library for interacting with BlueSky's Firehose
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR, Client

# Initialize the atproto client for querying user profiles
at_client = Client()

# Define a handler function to process each message received from the Firehose
def on_message_handler(message):
    # Parse the incoming message to extract the commit object
    commit = parse_subscribe_repos_message(message)
    
    # Ensure the commit is of the expected type (Commit)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    
    # Check if the commit contains any blocks (data chunks)
    if not commit.blocks:
        return
    
    # Create a CAR (Content-Addressed aRchive) object from the commit's blocks
    car = CAR.from_bytes(commit.blocks)
    
    # Extract the author's DID (Decentralized Identifier) from the commit
    author_did = commit.repo
    
    # Attempt to retrieve the author's profile to get their handle
    try:
        profile = at_client.query('com.atproto.identity.getProfile', data={'did': author_did})
        author_handle = profile['handle']
    except:
        # If profile retrieval fails (e.g., due to rate limits or errors), use the DID as the handle
        author_handle = author_did
    
    # Iterate through each operation in the commit
    for op in commit.ops:
        # Check if the operation is a "create" action and has a CID (Content Identifier)
        if op.action in ["create"] and op.cid:
            # Retrieve the data associated with the CID from the CAR object
            data = car.blocks.get(op.cid)
            
            # Verify if the data is a post of type 'app.bsky.feed.post' (BlueSky post type)
            if data and data['$type'] == 'app.bsky.feed.post':
                # Extract the text content of the post
                text = data.get('text', '')
                
                # Check if the post contains any hashtags (words starting with '#')
                if any(word.startswith('#') for word in text.split()):
                    # Print the author's handle and the post text if it contains hashtags
                    print(f"Author: {author_handle}\nPost: {text}\n")

# Initialize the Firehose client for subscribing to BlueSky's real-time event stream
firehose_client = FirehoseSubscribeReposClient()

# Start the Firehose client with the defined message handler
firehose_client.start(on_message_handler)