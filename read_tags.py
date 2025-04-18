from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR, Client

at_client = Client()

def on_message_handler(message):
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    if not commit.blocks:
        return
    car = CAR.from_bytes(commit.blocks)
    author_did = commit.repo
    try:
        profile = at_client.query('com.atproto.identity.getProfile', data={'did': author_did})
        author_handle = profile['handle']
    except:
        author_handle = author_did
    for op in commit.ops:
        if op.action in ["create"] and op.cid:
            data = car.blocks.get(op.cid)
            if data and data['$type'] == 'app.bsky.feed.post':
                text = data.get('text', '')
                words = text.split()
                hashtags = [word for word in words if word.startswith('#')]
                if hashtags:
                    print(f"Author: {author_handle}")
                    print(f"Post: {text}")
                    print(f"Hashtags: {', '.join(hashtags)}")
                    print()

firehose_client = FirehoseSubscribeReposClient()
firehose_client.start(on_message_handler)