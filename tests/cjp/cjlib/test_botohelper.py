import sys

src_dir = "../../src"  # TODO check path relative to execution

# Import the application and libraries for testing
sys.path.append(src_dir)

from databricks.sirens.cjp.cjlib.botohelper import boto_endpoints


def test_boto_endpoints():
    regions = {}
    # Catalog the regions in any of the services
    for service in boto_endpoints:
        for region in boto_endpoints[service]:
            regions[region] = True

    # Check that an HTTPS fips endpoint is present in each region for each service
    for service in boto_endpoints:
        for region in regions:
            assert region in boto_endpoints[service]
            endpoint = boto_endpoints[service][region]
            assert endpoint.find("https://") == 0
            assert endpoint.find("fips") > 0


# Note: Pytest does NOT require the code below, but it can be useful to run & terminate on error when debugging
if __name__ == "__main__":
    for token in dir():
        if token.find("test_") == 0:
            print(f"--- Running {token}()")
            locals()[token]()
