"""

Author: Derek King
Date: June 2024
Version: 1.0
"""

import tldextract


class DomainUtils:
    """
    A utility class for domain-related operations.
    """

    @staticmethod
    def is_valid_tld(url):
        """
        Check if the given URL has a valid top-level domain (TLD).

        :param url: The URL to check.
        :type url: str
        :return: True if the URL has a valid TLD, False otherwise.
        :rtype: bool
        """
        return bool(tldextract.extract(url.lower()).suffix)
