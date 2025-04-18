

# Import necessary classes and functions from the atproto library
# - FirehoseSubscribeReposClient: For subscribing to the BlueSky Firehose (real-time event stream)
# - parse_subscribe_repos_message: Function to parse messages from the Firehose
# - models: Contains data models for the AT Protocol
# - CAR: Represents Content-Addressed aRchives, used for storing and retrieving data
# - Client: General client for interacting with the BlueSky API
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR, Client

# Create an instance of the Client for making API queries, such as fetching user profiles
at_client = Client()

# Define a function to handle messages received from the Firehose
def on_message_handler(message):
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
                
                # Extract hashtags by filtering words that start with '#'
                hashtags = [word for word in words if word.startswith('#')]
                
                # If the post contains at least one hashtag, proceed to print the details
                if hashtags:
                    # Print the author's handle (or DID if handle retrieval failed)
                    print(f"Author: {author_handle}")
                    # Print the full text of the post
                    print(f"Post: {text}")
                    # Print the hashtags, comma-separated, on a new line
                    print(f"Hashtags: {', '.join(hashtags)}")
                    # Print a blank line for readability between posts
                    print()

# Create an instance of FirehoseSubscribeReposClient to connect to the BlueSky Firehose
firehose_client = FirehoseSubscribeReposClient()

# Start the Firehose client and begin processing messages using the on_message_handler function
firehose_client.start(on_message_handler)

