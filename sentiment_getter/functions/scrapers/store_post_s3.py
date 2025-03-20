"""
Store posts in S3 after scrapping as passing them as input to StepFunction State Machine
might cause issues with the size of the payload (max size is 256KB)
"""

from model.post import Post


def store_post_s3(posts: list[Post]) -> list[str]:
    """
    Store posts in S3, return list of post ids
    """

    for post in posts:
        post.persist()

    return [post.id for post in posts]
