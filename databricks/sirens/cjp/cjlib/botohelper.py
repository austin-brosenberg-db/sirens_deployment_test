from botocore.config import Config

boto_config = Config(retries={"max_attempts": 1}, s3={"addressing_style": "virtual"})

boto_endpoints = {
    "s3": {
        "us-east-1": "https://s3-fips.us-east-1.amazonaws.com",
        "us-east-2": "https://s3-fips.us-east-2.amazonaws.com",
        "us-west-1": "https://s3-fips.us-west-1.amazonaws.com",
        "us-west-2": "https://s3-fips.us-west-2.amazonaws.com",
    },
    "ssm": {
        "us-east-1": "https://ssm-fips.us-east-1.amazonaws.com",
        "us-east-2": "https://ssm-fips.us-east-2.amazonaws.com",
        "us-west-1": "https://ssm-fips.us-west-1.amazonaws.com",
        "us-west-2": "https://ssm-fips.us-west-2.amazonaws.com",
    },
}
